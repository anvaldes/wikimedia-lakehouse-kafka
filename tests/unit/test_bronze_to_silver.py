import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from processing.common import quality
from processing.common.iceberg import partition_predicate
from processing.contracts import recentchange_v1 as contract
from processing.jobs import bronze_to_silver as job

MIGRATION = Path(__file__).parents[2] / "processing" / "migrations" / "002_create_silver.sql"
REAL_CASES = ["edit_bot_wiktionary", "edit_temp_account_wikipedia", "new_page", "log_list_params_no_id",
              "log_object_params", "categorize_commons", "edit_wikidata"]


@pytest.fixture(scope="module")
def prepared(bronze_df):
    return job.prepare(bronze_df).cache()


@pytest.fixture(scope="module")
def by_case(prepared):
    return {row["case"]: row for row in prepared.collect()}


@pytest.fixture(scope="module")
def silver(bronze_df, prepared):
    cases = bronze_df.select("case", *job.KAFKA_KEY)
    rows = job.silver_rows(prepared).join(cases, job.KAFKA_KEY).collect()
    return {row["case"]: row for row in rows}


# ---------- contract ----------

def test_contract_required_fields_are_part_of_the_contract():
    assert set(contract.REQUIRED_FIELDS) <= set(contract.KNOWN_KEYS)
    assert {"meta.id", "meta.dt", "wiki", "type"} <= set(contract.REQUIRED_FIELDS)
    assert contract.CORRUPT_RECORD not in contract.KNOWN_KEYS
    assert contract.PARSE_OPTIONS["mode"] == "PERMISSIVE"


@pytest.mark.parametrize("case", REAL_CASES + ["drift_extra_fields"])
def test_real_events_pass_the_contract(by_case, case):
    assert by_case[case]["error_reason"] is None


def test_real_events_are_typed_by_the_contract(silver):
    edit = silver["edit_bot_wiktionary"]
    assert isinstance(edit["namespace"], int) and isinstance(edit["bot"], bool)
    assert edit["event_ts"] is not None and edit["rc_timestamp"] is not None
    assert edit["schema_version"] == "/mediawiki/recentchange/1.0.0"
    assert edit["contract_version"] == contract.VERSION
    log = silver["log_list_params_no_id"]
    assert log["rc_id"] is None                      # id is optional: null for some log events
    assert log["log_params"].startswith("[")         # arrays are kept as JSON text
    assert silver["log_object_params"]["log_params"].startswith("{")


# ---------- quarantine ----------

@pytest.mark.parametrize("case, reason", [
    ("invalid_json", "invalid_json"),
    ("invalid_type_namespace", "type_mismatch"),
    ("invalid_missing_wiki", "missing_wiki"),
])
def test_contract_violations_are_quarantined_with_their_reason(by_case, case, reason):
    assert by_case[case]["error_reason"] == reason


def test_quarantine_rows_follow_the_quarantine_contract(prepared):
    rows = job.quarantine_rows(prepared, datetime(2026, 11, 20, 19, tzinfo=timezone.utc), "test-1")
    assert rows.columns == job.QUARANTINE_COLUMNS
    collected = rows.collect()
    assert len(collected) == 3
    assert {r["run_hour"] for r in collected} == {"2026-11-20T19"}
    assert all(r["raw_json"] and r["kafka_offset"] is not None for r in collected)


def test_quarantined_rows_never_reach_silver(silver):
    assert not {"invalid_json", "invalid_type_namespace", "invalid_missing_wiki"} & set(silver)


# ---------- schema drift ----------

def test_schema_drift_reports_only_new_fields(prepared):
    assert job.detect_drift(prepared) == {"new_top_field": 1, "meta.new_meta_field": 1}


# ---------- dedupe ----------

def test_dedupe_keeps_the_copy_that_reached_kafka_first(silver, by_case):
    assert "duplicate_of_edit_temp_account_wikipedia" not in silver
    original = silver["edit_temp_account_wikipedia"]
    assert original["kafka_offset"] == by_case["edit_temp_account_wikipedia"]["kafka_offset"]


def test_silver_has_one_row_per_meta_id(prepared):
    rows = job.silver_rows(prepared)
    assert rows.count() == rows.select("meta_id").distinct().count() == len(REAL_CASES) + 1


# ---------- enrichment ----------

@pytest.mark.parametrize("case, project, language", [
    ("edit_bot_wiktionary", "wiktionary", "en"),
    ("edit_temp_account_wikipedia", "wikipedia", "tr"),
    ("new_page", "wikisource", "zh"),
    ("categorize_commons", "commons", None),
    ("edit_wikidata", "wikidata", None),
])
def test_project_and_language(silver, case, project, language):
    assert (silver[case]["project"], silver[case]["language"]) == (project, language)


def test_is_anonymous(silver):
    assert silver["edit_temp_account_wikipedia"]["is_anonymous"] is True
    assert silver["edit_bot_wiktionary"]["is_anonymous"] is False


def test_length_delta_and_bucket(silver):
    edit = silver["edit_bot_wiktionary"]
    assert edit["length_delta"] == edit["length_new"] - edit["length_old"]
    new = silver["new_page"]
    assert new["length_old"] is None and new["length_delta"] == new["length_new"]
    assert new["edit_size_bucket"] in {"tiny", "small", "medium", "large", "huge"}
    log = silver["log_object_params"]
    assert log["length_delta"] is None and log["edit_size_bucket"] is None


@pytest.mark.parametrize("delta, bucket", [(0, "none"), (-50, "tiny"), (51, "small"), (5000, "medium"),
                                           (-50000, "large"), (50001, "huge"), (None, None)])
def test_edit_size_bucket_limits(spark, delta, bucket):
    df = spark.createDataFrame([(delta,)], "delta int")
    assert df.select(job.edit_size_bucket(F.col("delta"))).first()[0] == bucket


def test_namespace_name(silver):
    assert silver["categorize_commons"]["namespace_name"] == "Category"
    assert silver["edit_wikidata"]["namespace_name"] in job.NAMESPACES.values() or \
        silver["edit_wikidata"]["namespace_name"] == "Other"


# ---------- output contract ----------

def ddl_columns(table):
    sql = MIGRATION.read_text()
    block = sql.split(f"CREATE TABLE IF NOT EXISTS {table} (")[1].split("\n)")[0]
    return [re.match(r"\s*`?(\w+)`?\s", line).group(1) for line in block.splitlines() if line.strip()]


def test_job_columns_match_the_ddl():
    assert ddl_columns("lakehouse.silver.recentchange") == job.SILVER_COLUMNS
    assert ddl_columns("lakehouse.silver.recentchange_quarantine") == job.QUARANTINE_COLUMNS


def test_silver_rows_can_be_written_with_the_silver_contract(prepared):
    stamped = job.stamp_silver(job.silver_rows(prepared), job.utc_now())
    row = stamped.select(*job.SILVER_COLUMNS).first()
    assert row["timestamp_silver"] >= row["timestamp_bronze"]
    assert row["latency_to_silver"] >= 0 and row["latency_processing_silver"] >= 0


# ---------- quality ----------

def run_job_checks(prepared, quarantined, read, scope_hours=None):
    counts = {"read": read, "quarantined": quarantined}
    return quality.run_checks(job.silver_rows(prepared), job.build_checks(counts, scope_hours))


def test_quality_checks_pass_on_a_clean_batch(prepared):
    results = run_job_checks(prepared, quarantined=3, read=10_000)
    quality.enforce(results)  # min_volume fails but is not critical in micro-batches
    assert [r.name for r in results if not r.passed] == []


def test_quarantine_over_one_percent_fails_the_batch(prepared):
    results = run_job_checks(prepared, quarantined=3, read=12)
    with pytest.raises(quality.QualityCheckError, match="quarantine_ratio"):
        quality.enforce(results)


def test_min_volume_is_critical_only_for_full_hours(prepared):
    micro = run_job_checks(prepared, quarantined=0, read=100)
    quality.enforce(micro)  # only warns
    hourly = run_job_checks(prepared, quarantined=0, read=100, scope_hours=2)
    with pytest.raises(quality.QualityCheckError, match="min_volume"):
        quality.enforce(hourly)


def test_quality_null_ratio_and_uniqueness(spark):
    df = spark.createDataFrame([("a", None), ("a", "x"), (None, "y")], "id string, title string")
    results = {r.name: r for r in quality.run_checks(df, [
        quality.not_null("id"), quality.unique("id"), quality.max_null_ratio("title", 0.5)])}
    assert results["id_not_null"].value == 1 and not results["id_not_null"].passed
    assert results["id_unique"].value == 1 and not results["id_unique"].passed
    assert results["title_null_ratio"].passed


# ---------- scope ----------

def test_backfill_scope_is_h_and_h_minus_one_across_days():
    partitions = job.scope_partitions(datetime(2026, 11, 21, 0, tzinfo=timezone.utc))
    assert partitions == [{"year": "2026", "month": "11", "day": "20", "hour": "23"},
                          {"year": "2026", "month": "11", "day": "21", "hour": "00"}]
    assert partition_predicate(partitions[:1], "t") == \
        "(t.year = '2026' AND t.month = '11' AND t.day = '20' AND t.hour = '23')"
