"""silver_to_gold: one test block per gold table + quality checks.

Fixture tests/fixtures/silver_rows.jsonl (H = 2026-11-21T10Z):
  enwiki  H:   'Spike page' 12 edits by 6 users (2 anonymous) vs 2 edits in the 24 h baseline -> anomaly
               'Steady page' 10 bot edits every hour H-24 .. H -> z = 0, not an anomaly
               'Page 01'..'Page 25' 1 edit each (more than 20 trending candidates), a new page, a large removal,
               a 'User talk' edit (not an article), a log and a categorize event
  enwiki  H-1: 'Earlier page' 3 edits + 'Steady page'
  eswiki  H:   anonymous (temporary account), registered and bot edits + a new page
  commonswiki H: bot categorize events and a File edit (language is null)
"""
import json
import math
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, StringType, StructField, StructType, TimestampType)

from processing.common import quality
from processing.jobs import bronze_to_silver
from processing.jobs import silver_to_gold as job

H = datetime(2026, 11, 21, 10, tzinfo=timezone.utc)
PROCESSED_AT = datetime(2026, 11, 21, 11, 10, tzinfo=timezone.utc)
FIXTURE = Path(__file__).parents[1] / "fixtures" / "silver_rows.jsonl"
MIGRATION = Path(__file__).parents[2] / "processing" / "migrations" / "003_create_gold.sql"

SILVER_SCHEMA = StructType([
    StructField("meta_id", StringType()), StructField("event_ts", TimestampType()),
    StructField("wiki", StringType()), StructField("project", StringType()), StructField("language", StringType()),
    StructField("type", StringType()), StructField("namespace_name", StringType()), StructField("title", StringType()),
    StructField("user", StringType()), StructField("bot", BooleanType()), StructField("is_anonymous", BooleanType()),
    StructField("minor", BooleanType()), StructField("length_delta", IntegerType()),
    StructField("edit_size_bucket", StringType()),
    *[StructField(name, StringType()) for name in ("year", "month", "day", "hour")],
])


@pytest.fixture(scope="module")
def window(spark):
    """Silver rows of H-24 .. H, as read_silver() returns them."""
    rows = []
    for line in FIXTURE.read_text().splitlines():
        row = json.loads(line)
        row["event_ts"] = datetime.fromisoformat(row["event_ts"])
        rows.append(tuple(row[field.name] for field in SILVER_SCHEMA.fields))
    return spark.createDataFrame(rows, SILVER_SCHEMA).select(*job.SILVER_FIELDS).cache()


@pytest.fixture(scope="module")
def tables(window):
    """Backfill run for H: aggregates for H-1 and H, anomalies for H."""
    built = job.build_tables(window, job.backfill_scope(H), PROCESSED_AT)
    return {table.name: df.cache() for table, df in built.items()}


@pytest.fixture(scope="module")
def scope(window):
    """Silver rows of the recomputed hours (what run_quality compares activity_hourly against)."""
    return job.scope_rows(window, job.backfill_scope(H))


def rows(df, **filters):
    for column, value in filters.items():
        df = df.filter(F.col(column).isNull() if value is None else F.col(column) == value)
    return df.collect()


def one(df, **filters):
    found = rows(df, **filters)
    assert len(found) == 1, f"expected 1 row for {filters}, got {len(found)}"
    return found[0]


# ---------- contracts ----------

def test_silver_provides_every_field_gold_uses():
    assert set(job.SILVER_FIELDS) <= set(bronze_to_silver.SILVER_COLUMNS)


def ddl_columns(table):
    block = MIGRATION.read_text().split(f"CREATE TABLE IF NOT EXISTS {table} (")[1].split("\n)")[0]
    return [re.match(r"\s*`?(\w+)`?\s", line).group(1) for line in block.splitlines() if line.strip()]


@pytest.mark.parametrize("table", job.TABLES, ids=lambda t: t.name)
def test_job_columns_match_the_ddl(table):
    assert ddl_columns(table.table) == table.columns


@pytest.mark.parametrize("table", job.TABLES, ids=lambda t: t.name)
def test_tables_follow_their_contract(tables, table):
    assert tables[table.name].columns == table.columns


def test_unique_counts_are_documented_as_not_summable():
    sql = MIGRATION.read_text()
    for line in sql.splitlines():
        if re.match(r"\s+unique_(editors|pages)\s", line):
            assert "NOT summable" in line


# ---------- scope ----------

def test_backfill_scope_is_h_and_h_minus_one_with_anomalies_only_for_h():
    scope = job.backfill_scope(H)
    assert [p["hour"] for p in scope.partitions_of(job.ACTIVITY)] == ["09", "10"]
    assert [p["hour"] for p in scope.partitions_of(job.ANOMALIES)] == ["10"]
    reads = scope.read_partitions()                    # H-24 .. H: the anomaly baseline is the widest window
    assert len(reads) == 25 and reads[0] == {"year": "2026", "month": "11", "day": "20", "hour": "10"}


def test_stream_scope_recomputes_every_table_for_the_batch_hours():
    late, current = H - timedelta(hours=5), H
    scope = job.stream_scope([current, late])
    assert scope.hours == scope.anomaly_hours == [late, current]
    assert len(scope.read_partitions()) == 25 + 5      # union of both baselines, no duplicates


def test_affected_hours_of_a_micro_batch(window):
    micro_batch = window.filter("hour IN ('09', '10') AND day = '21'").select(*job.PARTITIONS)
    assert sorted(job.affected_hours(micro_batch)) == [H - timedelta(hours=1), H]


# ---------- activity_hourly ----------

def test_activity_counts(tables):
    en = one(tables["activity_hourly"], wiki="enwiki", hour="10")
    assert (en.total_events, en.edits, en.new_pages, en.log_events, en.categorize_events) == (52, 49, 1, 1, 1)
    assert (en.unique_editors, en.unique_pages, en.minor_edits) == (16, 32, 1)
    assert en.anonymous_edits == 5 and en.anonymous_pct == pytest.approx(5 / 50)
    assert (en.bytes_added, en.bytes_removed, en.net_bytes) == (1370, 5000, -3630)
    assert en.gold_processed_at is not None
    # collect() converts timestamps to the machine's local time zone: compare in Spark (session time zone = UTC)
    hour_ts = (tables["activity_hourly"].filter("wiki = 'enwiki' AND hour = '10'")
               .select(F.date_format("hour_ts", "yyyy-MM-dd HH:mm:ss")).first()[0])
    assert hour_ts == "2026-11-21 10:00:00"


def test_activity_covers_h_minus_one_and_null_language(tables):
    assert one(tables["activity_hourly"], wiki="enwiki", hour="09").total_events == 13
    es = one(tables["activity_hourly"], wiki="eswiki", hour="10")
    assert es.anonymous_pct == pytest.approx(2 / 5) and es.net_bytes == 1040
    commons = one(tables["activity_hourly"], wiki="commonswiki")
    assert commons.language is None and commons.project == "commons" and commons.anonymous_pct == 0.0


def test_activity_only_writes_h_and_h_minus_one(tables):
    hours = {r.hour for r in tables["activity_hourly"].select("hour").distinct().collect()}
    assert hours == {"09", "10"}


# ---------- bots_vs_humans_hourly ----------

def test_bots_vs_humans_split(tables):
    df = tables["bots_vs_humans_hourly"]
    by_type = {r.editor_type: r for r in rows(df, wiki="enwiki", hour="10")}
    assert {t: r.events for t, r in by_type.items()} == {"bot": 10, "human_anonymous": 5, "human_registered": 37}
    assert by_type["bot"].pct_of_wiki_events == pytest.approx(10 / 52)
    assert sum(r.pct_of_wiki_events for r in by_type.values()) == pytest.approx(1.0)


# ---------- trending_pages_hourly ----------

def test_trending_keeps_top_20_articles(tables):
    en = {r.title: r for r in rows(tables["trending_pages_hourly"], wiki="enwiki", hour="10")}
    assert len(en) == 20
    assert en["Spike page"].rank_in_wiki == 1 and en["Spike page"].unique_editors == 6
    steady = en["Steady page"]
    assert (steady.rank_in_wiki, steady.bot_edits, steady.human_edits) == (2, 10, 0)
    assert "User talk:X" not in en                     # not an article
    assert {r.rank_in_wiki for t, r in en.items() if t not in ("Spike page", "Steady page")} == {3}
    assert "New article" in en and "Page 17" in en and "Page 18" not in en  # ties cut by title


def test_trending_ignores_non_article_namespaces(tables):
    assert rows(tables["trending_pages_hourly"], wiki="commonswiki") == []
    es = {r.title: r.rank_in_wiki for r in rows(tables["trending_pages_hourly"], wiki="eswiki")}
    assert es == {"Página": 1, "Nueva": 2}


# ---------- edit_size_hourly ----------

def test_edit_size_uses_silver_buckets_and_only_edits(tables):
    df = tables["edit_size_hourly"]
    en = {r.edit_size_bucket: r for r in rows(df, wiki="enwiki", hour="10")}
    assert {b: r.edits for b, r in en.items()} == {"none": 10, "tiny": 38, "medium": 1}
    assert en["medium"].avg_length_delta == -5000
    assert sum(r.pct_of_wiki_edits for r in en.values()) == pytest.approx(1.0)
    es = {r.edit_size_bucket: r.edits for r in rows(df, wiki="eswiki")}
    assert es == {"tiny": 3, "small": 1}               # the new page (800) is not an edit


# ---------- anomalies_hourly ----------

def test_anomalies_flags_only_the_spike(tables):
    found = rows(tables["anomalies_hourly"])
    assert [(r.wiki, r.title) for r in found] == [("enwiki", "Spike page")]
    spike = found[0]
    avg = 2 / 24
    std = math.sqrt(2 / 24 - avg ** 2)
    assert spike.edits_in_hour == 12 and spike.unique_editors == 6 and spike.hour == "10"
    assert spike.baseline_avg_edits == pytest.approx(avg)
    assert spike.baseline_stddev == pytest.approx(std)
    assert spike.z_score == pytest.approx((12 - avg) / std) and spike.is_anomaly


def test_scoring_several_hours_at_once_matches_scoring_each_hour(window):
    """Streaming scores all the batch's hours in one pass: H must score exactly as when scored alone."""
    columns = ["wiki", "title", "edits_in_hour", "baseline_avg_edits", "baseline_stddev", "z_score", "is_anomaly"]
    together = job.score_pages(window, [H - timedelta(hours=1), H]).filter("hour = '10'").select(*columns)
    alone = job.score_pages(window, [H]).select(*columns)
    assert sorted(together.collect()) == sorted(alone.collect())
    earlier = {r.title: r for r in job.score_pages(window, [H - timedelta(hours=1)]).collect()}
    assert not earlier["Steady page"].is_anomaly and "Spike page" not in earlier


def test_steady_page_has_zero_stddev_and_is_not_an_anomaly(window):
    scored = {(r.wiki, r.title): r for r in job.score_pages(window, [H]).collect()}
    steady = scored[("enwiki", "Steady page")]
    assert (steady.baseline_avg_edits, steady.baseline_stddev, steady.z_score) == (10.0, 0.0, 0.0)
    assert not steady.is_anomaly
    assert not scored[("eswiki", "Página")].is_anomaly  # z > 3 but fewer than 10 edits


# ---------- quality ----------

def test_all_checks_pass_on_the_fixture(tables, scope):
    results = job.run_quality({t: tables[t.name] for t in job.TABLES}, scope)
    assert [r.name for r in results if not r.passed] == []
    names = {r.name for r in results}
    assert {"activity_hourly.total_events_match_silver", "bots_vs_humans_hourly.pct_of_wiki_events_sums_to_1",
            "edit_size_hourly.pct_of_wiki_edits_sums_to_1", "trending_pages_hourly.max_rows_per_group",
            "anomalies_hourly.no_duplicate_keys", "activity_hourly.no_negative_counts"} <= names
    quality.enforce(results)


def failed(results):
    return {r.name for r in results if not r.passed}


def test_activity_total_mismatch_fails(tables, scope):
    broken = tables["activity_hourly"].withColumn("total_events", F.col("total_events") + 1)
    results = job.run_quality({job.ACTIVITY: broken}, scope)
    assert failed(results) == {"activity_hourly.total_events_match_silver"}
    with pytest.raises(quality.QualityCheckError):
        quality.enforce(results)


def test_duplicate_keys_and_negative_counts_fail(tables, scope):
    df = tables["edit_size_hourly"]
    broken = df.unionByName(df.limit(1).withColumn("edits", F.lit(-1)))
    assert failed(job.run_quality({job.EDIT_SIZE: broken}, scope)) == {
        "edit_size_hourly.no_negative_counts", "edit_size_hourly.no_duplicate_keys",
        "edit_size_hourly.pct_of_wiki_edits_sums_to_1"}


def test_percentages_must_sum_to_one(tables, scope):
    broken = tables["bots_vs_humans_hourly"].withColumn("pct_of_wiki_events", F.col("pct_of_wiki_events") * 0.9)
    assert failed(job.run_quality({job.BOTS_VS_HUMANS: broken}, scope)) == {
        "bots_vs_humans_hourly.pct_of_wiki_events_sums_to_1"}


def test_more_than_20_trending_rows_fails(spark, tables, scope):
    df = tables["trending_pages_hourly"]
    extra = df.limit(1).withColumn("title", F.lit("Page 99"))
    assert failed(job.run_quality({job.TRENDING: df.unionByName(extra)}, scope)) == {
        "trending_pages_hourly.max_rows_per_group"}
