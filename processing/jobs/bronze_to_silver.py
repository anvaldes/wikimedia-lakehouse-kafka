"""Bronze (Iceberg) -> Silver (Iceberg).

Modes (same as kafka_to_bronze):
  continuous (real time): Structured Streaming over the bronze table's new snapshots, a micro-batch every
                          CONTINUOUS_TRIGGER, never ends. Late events are picked up whatever hour they fall in.
  stream     (scheduled): same source + Trigger.AvailableNow: processes everything new and exits.
  backfill   (replay):    batch read of the bronze partitions of hour H and H-1 (H = --year/--month/--day/--hour),
                          so late events that landed in H-1 are absorbed.
continuous and stream share the Spark checkpoint, so you can switch between them, but never run both at once.
In continuous/stream the checkpoint decides what to read; --year/--month/--day/--hour only label the run.

Per batch:
  1. parse raw_json with contracts/recentchange_v1 (explicit schema, PERMISSIVE + corrupt record column)
  2. schema drift: JSON keys not in the contract -> "schema_drift" log + metric (never fails the job)
  3. quarantine: contract violations -> silver.recentchange_quarantine + DLQ topic, BEFORE the quality checks
  4. flatten, type, enrich, dedupe by meta_id within the batch (earliest Kafka timestamp/offset wins)
  5. quality checks: any critical failure -> no MERGE, the job fails
  6. insert-only MERGE on meta_id, bounded to the batch's partitions

Idempotency: re-running any batch or hour inserts nothing new (insert-only on meta_id in silver and on the Kafka
key in quarantine). The DLQ only gets quarantine rows not already in the table (at-least-once on a crash).
"""
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import Window
from pyspark.sql import functions as F

from processing.common import config, quality
from processing.common.args import parse_args, run_hour
from processing.common.columns import PARTITIONS, ms_between, ms_since, utc_now
from processing.common.iceberg import (added_records, distinct_partitions, latest_snapshot, maintain,
                                       merge_insert_only, partition_predicate)
from processing.common.lineage import emit_lineage, gcs_dataset, kafka_dataset
from processing.common.metrics import Metrics
from processing.common.migrations import run_migrations
from processing.common.spark_session import create_spark, log_versions
from processing.common.streaming import stream_tag, stream_trigger, write_kafka
from processing.contracts import recentchange_v1 as contract
from shared.partitions import get_partition

JOB_NAME = "bronze_to_silver"
METRIC = JOB_NAME  # metric name prefix
MIGRATIONS = ["002_create_silver.sql"]
TABLES = [config.SILVER_TABLE, config.SILVER_QUARANTINE_TABLE]

# ---------- contracts of the tables this job reads and writes ----------

KAFKA_KEY = ["kafka_topic", "kafka_partition", "kafka_offset"]
BRONZE_COLUMNS = [
    "raw_json", *KAFKA_KEY, "ingested_at", "timestamp_kafka", "timestamp_bronze",
    "latency_to_kafka", "latency_to_bronze", "latency_processing_bronze", *PARTITIONS,
]
SILVER_COLUMNS = [
    "meta_id", "event_ts", "schema_version", "contract_version",
    "rc_id", "type", "wiki", "server_name", "server_url", "server_script_path", "project", "language",
    "namespace", "namespace_name", "title", "title_url", "user", "is_anonymous", "bot", "minor", "patrolled",
    "comment", "rc_timestamp",
    "length_old", "length_new", "length_delta", "edit_size_bucket", "revision_old", "revision_new",
    "log_id", "log_type", "log_action", "log_params", "log_action_comment",
    "meta_uri", "meta_request_id", "meta_domain", "meta_stream", "meta_topic", "meta_partition", "meta_offset",
    *[c for c in BRONZE_COLUMNS if c not in PARTITIONS],
    "timestamp_silver", "latency_to_silver", "latency_processing_silver",
    *PARTITIONS,
]
QUARANTINE_COLUMNS = [
    "error_reason", "raw_json", *KAFKA_KEY, "timestamp_kafka",
    "run_hour", "failed_at", "job", "batch", "contract_version", *PARTITIONS,
]

# ---------- quality thresholds ----------

QUARANTINE_MAX_RATIO = 0.01
# wiki/type are required by the contract (always 0 here: a guard). title is optional in the contract but was
# present in 100% of 21k real events, so >1% nulls means something upstream changed.
NULL_MAX_RATIO = 0.01
NULL_CHECKED_COLUMNS = ["wiki", "type", "title"]
# The stream brings ~25-50 events/s = 90k-180k/hour. Below 30k/hour (~8/s) the producer is almost certainly
# broken. Critical in backfill (full hours). In continuous/stream a micro-batch holds whatever bronze committed
# since the last one, so the volume check only warns (it would otherwise kill the 24/7 job on a slow minute).
MIN_ROWS_PER_HOUR = 30_000
MIN_ROWS_PER_MICRO_BATCH = 500

# ---------- enrichment reference data ----------

# Canonical namespaces (same id on every wiki) + those defined by extensions deployed on all wikis.
# Ids >= 100 not listed here vary per wiki (e.g. 104 is Page on Wikisource but Reconstruction on Wiktionary).
NAMESPACES = {
    -2: "Media", -1: "Special", 0: "Main", 1: "Talk", 2: "User", 3: "User talk", 4: "Project",
    5: "Project talk", 6: "File", 7: "File talk", 8: "MediaWiki", 9: "MediaWiki talk", 10: "Template",
    11: "Template talk", 12: "Help", 13: "Help talk", 14: "Category", 15: "Category talk",
    710: "TimedText", 711: "TimedText talk", 828: "Module", 829: "Module talk",
    2300: "Gadget", 2301: "Gadget talk", 2302: "Gadget definition", 2303: "Gadget definition talk",
}
# project / language come from the wiki's database name, NOT from server_name: account creations and some
# abuse-filter logs of every wiki arrive with server_name = auth.wikimedia.org (central login), which split one
# wiki into two projects. Checked against the 635 real wiki/server_name pairs in tests/fixtures/wikis.csv.
# Wikis with one edition per language: <language><suffix>, e.g. eswiktionary. Plain <language>wiki = Wikipedia.
LANGUAGE_PROJECTS = ["wiktionary", "wikibooks", "wikinews", "wikiquote", "wikisource", "wikiversity", "wikivoyage"]
# Single-language / multilingual wikis whose name doesn't follow <language><project>.
SPECIAL_WIKIS = {
    "commonswiki": "commons", "metawiki": "meta", "specieswiki": "species", "incubatorwiki": "incubator",
    "foundationwiki": "foundation", "outreachwiki": "outreach", "donatewiki": "donate", "loginwiki": "login",
    "labswiki": "wikitech", "mediawikiwiki": "mediawiki", "wikidatawiki": "wikidata",
    "testwikidatawiki": "wikidata", "wikifunctionswiki": "wikifunctions", "sourceswiki": "wikisource",
}
# Database names that kept an old language code (be_x_oldwiki serves be-tarask.wikipedia.org).
LANGUAGE_CODE_FIXES = {"be-x-old": "be-tarask"}
# IPv4, IPv6, or temporary account (~2026-12345-67). Real events: 100% of anonymous edits are temporary accounts.
ANONYMOUS_USER = r"^(\d{1,3}(\.\d{1,3}){3}|[0-9A-Fa-f]*:[0-9A-Fa-f:]+|~\d{4}-.*)$"
EDIT_SIZE_BUCKETS = [(0, "none"), (50, "tiny"), (500, "small"), (5_000, "medium"), (50_000, "large")]


@dataclass
class Run:
    spark: object
    mode: str
    hour: datetime
    metrics: Metrics
    scope_hours: Optional[int]  # full hours per batch (backfill) or None (micro-batches)


# ---------- parse ----------

def field(path):
    """Column for a contract path such as 'meta.id' or '$schema' inside the parsed event."""
    column = F.col("event")
    for part in path.split("."):
        column = column.getField(part)
    return column


def is_missing(path):
    value = field(path)
    return value.isNull() | (F.trim(value.cast("string")) == "")


def error_reason():
    """First contract violation of the row, or null if the row is valid."""
    raw = F.col("raw_json")
    reason = F.when(raw.isNull() | (F.trim(raw) == ""), "empty_value")
    reason = reason.when(F.get_json_object(raw, "$").isNull(), "invalid_json")
    reason = reason.when(~F.trim(raw).startswith("{"), "not_json_object")
    reason = reason.when(field(contract.CORRUPT_RECORD).isNotNull(), "type_mismatch")
    for path in contract.REQUIRED_FIELDS:
        reason = reason.when(is_missing(path), f"missing_{path}")
    return reason.when(F.col("event_ts").isNull(), "invalid_meta.dt")


def prepare(bronze_df):
    df = bronze_df.withColumn("event", F.from_json("raw_json", contract.SCHEMA, contract.PARSE_OPTIONS))
    df = df.withColumn("event_ts", F.try_to_timestamp(field("meta.dt")))
    return df.withColumn("error_reason", error_reason())


def count_rows(prepared):
    rows = prepared.groupBy(F.col("error_reason").isNull().alias("valid")).count().collect()
    counts = {row["valid"]: row["count"] for row in rows}
    return {"read": sum(counts.values()), "quarantined": counts.get(False, 0)}


# ---------- schema drift ----------

def detect_drift(prepared):
    """{key: rows} for top-level and meta.* keys present in the JSON but not in the contract."""
    empty = F.array().cast("array<string>")
    top = F.coalesce(F.expr("json_object_keys(raw_json)"), empty)
    meta = F.coalesce(F.expr("transform(json_object_keys(get_json_object(raw_json, '$.meta')), k -> concat('meta.', k))"), empty)
    keys = prepared.select(F.explode(F.concat(top, meta)).alias("key"))
    unknown = keys.filter(~F.col("key").isin(contract.KNOWN_KEYS)).groupBy("key").count()
    return {row["key"]: row["count"] for row in unknown.collect()}


def report_drift(drift, tag, metrics):
    if not drift:
        return
    print(json.dumps({"log": "schema_drift", "job": JOB_NAME, "batch": tag,
                      "contract": contract.VERSION, "fields": drift}))
    metrics.add(f"{METRIC}.schema_drift_fields", len(drift))


# ---------- quarantine ----------

def quarantine_rows(prepared, hour, tag):
    return prepared.filter(F.col("error_reason").isNotNull()).select(
        "error_reason", "raw_json", *KAFKA_KEY, "timestamp_kafka",
        F.lit(f"{hour:%Y-%m-%dT%H}").alias("run_hour"),
        F.lit(utc_now()).alias("failed_at"),
        F.lit(JOB_NAME).alias("job"),
        F.lit(tag).alias("batch"),
        F.lit(contract.VERSION).alias("contract_version"),
        *PARTITIONS,
    )


def write_quarantine(spark, quarantine):
    """Only rows not already quarantined: DLQ first, then the table. A crash in between re-sends them to the
    DLQ on retry (at-least-once) but never loses them. Returns the number of new rows."""
    existing = (spark.table(config.SILVER_QUARANTINE_TABLE)
                .where(partition_predicate(distinct_partitions(quarantine)))
                .select(*PARTITIONS, *KAFKA_KEY))
    new = quarantine.join(existing, [*PARTITIONS, *KAFKA_KEY], "left_anti").select(*QUARANTINE_COLUMNS).persist()
    try:
        count = new.count()
        if count:
            write_kafka(new, config.SILVER_DLQ_TOPIC, QUARANTINE_COLUMNS)
            new.writeTo(config.SILVER_QUARANTINE_TABLE).append()
        return count
    finally:
        new.unpersist()


# ---------- transform ----------

def flatten(valid):
    return valid.select(
        field("meta.id").alias("meta_id"),
        "event_ts",
        field("$schema").alias("schema_version"),
        F.lit(contract.VERSION).alias("contract_version"),
        field("id").alias("rc_id"),
        *[field(name).alias(name) for name in [
            "type", "wiki", "server_name", "server_url", "server_script_path", "namespace", "title",
            "title_url", "user", "bot", "minor", "patrolled", "comment",
            "log_id", "log_type", "log_action", "log_params", "log_action_comment"]],
        F.timestamp_seconds(field("timestamp")).alias("rc_timestamp"),
        field("length.old").alias("length_old"),
        field("length.new").alias("length_new"),
        field("revision.old").alias("revision_old"),
        field("revision.new").alias("revision_new"),
        *[field(f"meta.{name}").alias(f"meta_{name}") for name in
          ["uri", "request_id", "domain", "stream", "topic", "partition", "offset"]],
        *BRONZE_COLUMNS,
    )


def literal_map(mapping):
    return F.create_map(*[F.lit(x) for pair in mapping.items() for x in pair])


def project_and_language(wiki):
    """From the wiki database name, so both are constant per wiki:
    enwiki -> (wikipedia, en). eswiktionary -> (wiktionary, es). zh_min_nanwiki -> (wikipedia, zh-min-nan).
    commonswiki -> (commons, null). wikimania2017wiki -> (wikimania2017, null). bdwikimedia (chapter) -> (wikimedia, null).
    Anything else -> (wiki, null)."""
    special = literal_map(SPECIAL_WIKIS)[wiki]
    wikimania = F.regexp_extract(wiki, r"^(wikimania\d*)wiki$", 1)
    is_chapter = wiki.rlike(r"^[a-z_]+wikimedia$")
    by_language = rf"^([a-z0-9_]+?)({'|'.join(LANGUAGE_PROJECTS)})$"
    suffix, suffix_language = F.regexp_extract(wiki, by_language, 2), F.regexp_extract(wiki, by_language, 1)
    wikipedia_language = F.regexp_extract(wiki, r"^([a-z0-9_]+)wiki$", 1)
    project = (F.when(special.isNotNull(), special)
                .when(wikimania != "", wikimania)
                .when(is_chapter, "wikimedia")
                .when(suffix != "", suffix)
                .when(wikipedia_language != "", "wikipedia")
                .otherwise(wiki))
    code = (F.when(special.isNotNull() | (wikimania != "") | is_chapter, F.lit(None).cast("string"))
             .when(suffix != "", suffix_language)
             .when(wikipedia_language != "", wikipedia_language))
    code = F.regexp_replace(code, "_", "-")
    return project, F.coalesce(literal_map(LANGUAGE_CODE_FIXES)[code], code)


def namespace_name(namespace):
    mapping = F.create_map(*[F.lit(x) for pair in NAMESPACES.items() for x in pair])
    return F.when(namespace.isNotNull(), F.coalesce(mapping[namespace], F.lit("Other")))


def edit_size_bucket(delta):
    size = F.abs(delta)
    bucket = F.when(size.isNull(), F.lit(None).cast("string"))
    for limit, name in EDIT_SIZE_BUCKETS:
        bucket = bucket.when(size <= limit, name)
    return bucket.otherwise("huge")


def enrich(df):
    project, language = project_and_language(F.col("wiki"))
    length_delta = F.when(F.col("length_new").isNotNull(),
                          F.col("length_new") - F.coalesce(F.col("length_old"), F.lit(0)))
    return (df.withColumn("project", project)
              .withColumn("language", language)
              .withColumn("namespace_name", namespace_name(F.col("namespace")))
              .withColumn("is_anonymous", F.col("user").rlike(ANONYMOUS_USER))
              .withColumn("length_delta", length_delta.cast("int"))
              .withColumn("edit_size_bucket", edit_size_bucket(F.col("length_delta"))))


def dedupe(df):
    """One row per meta_id: the copy that reached Kafka first (ties broken by Kafka position, deterministic)."""
    window = Window.partitionBy("meta_id").orderBy("timestamp_kafka", *KAFKA_KEY)
    return df.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")


def silver_rows(prepared):
    return dedupe(enrich(flatten(prepared.filter(F.col("error_reason").isNull()))))


def stamp_silver(df, started):
    """timestamp_silver = processing finished; latency_processing_silver = how long that processing took."""
    df = df.withColumn("timestamp_silver", F.lit(utc_now()))
    df = df.withColumn("latency_to_silver", ms_between("timestamp_silver", "timestamp_bronze"))
    return df.withColumn("latency_processing_silver", F.lit(ms_since(started)).cast("long"))


# ---------- quality ----------

def build_checks(counts, scope_hours):
    checks = [
        quality.not_null("meta_id"),
        quality.unique("meta_id"),
        quality.not_null("event_ts"),
        quality.max_ratio("quarantine_ratio", counts["quarantined"], counts["read"], QUARANTINE_MAX_RATIO),
        *[quality.max_null_ratio(column, NULL_MAX_RATIO) for column in NULL_CHECKED_COLUMNS],
    ]
    if scope_hours:
        checks.append(quality.min_value("min_volume", counts["read"], MIN_ROWS_PER_HOUR * scope_hours))
    else:
        checks.append(quality.min_value("min_volume", counts["read"], MIN_ROWS_PER_MICRO_BATCH, critical=False))
    return checks


def run_quality(silver, counts, run, tag):
    results = quality.run_checks(silver, build_checks(counts, run.scope_hours))
    quality.log_results(results, JOB_NAME, tag)
    for r in results:
        run.metrics.add(f"{METRIC}.checks_passed" if r.passed else f"{METRIC}.checks_failed", 1, check=r.name)
    quality.enforce(results)


# ---------- write ----------

def merge_into_silver(spark, silver):
    previous_snapshot, _ = latest_snapshot(spark, config.SILVER_TABLE)
    merge_insert_only(silver.select(*SILVER_COLUMNS), config.SILVER_TABLE, ["meta_id"],
                      distinct_partitions(silver), "silver_batch")
    return added_records(spark, config.SILVER_TABLE, previous_snapshot)


# ---------- batch ----------

def report_batch(counts, stats, started, tag, metrics):
    elapsed = ms_since(started)
    print(f"✔ Batch {tag}: {counts['read']} read, {counts['quarantined']} quarantined "
          f"({stats['quarantine_new']} new), {stats['duplicates']} duplicates, {stats['written']} written, {elapsed} ms")
    metrics.add(f"{METRIC}.rows_read", counts["read"])
    metrics.add(f"{METRIC}.rows_quarantined", stats["quarantine_new"])
    metrics.add(f"{METRIC}.duplicates_removed", stats["duplicates"])
    metrics.add(f"{METRIC}.rows_written", stats["written"])
    metrics.record_ms(f"{METRIC}.batch_duration_ms", elapsed)


def process_batch(bronze_df, run, tag):
    started = utc_now()
    prepared = prepare(bronze_df).persist()
    try:
        counts = count_rows(prepared)
        if not counts["read"]:
            print(f"✔ Batch {tag}: no new rows")
            return
        report_drift(detect_drift(prepared), tag, run.metrics)
        stats = {"quarantine_new": 0, "duplicates": 0, "written": 0}
        if counts["quarantined"]:
            stats["quarantine_new"] = write_quarantine(run.spark, quarantine_rows(prepared, run.hour, tag))
        silver = silver_rows(prepared).persist()
        try:
            stats["duplicates"] = counts["read"] - counts["quarantined"] - silver.count()
            run_quality(silver, counts, run, tag)
            stats["written"] = merge_into_silver(run.spark, stamp_silver(silver, started))
        finally:
            silver.unpersist()
        report_batch(counts, stats, started, tag, run.metrics)
    finally:
        prepared.unpersist()


# ---------- modes ----------

def read_bronze_stream(spark):
    """Only append snapshots are read (bronze is written with insert-only MERGE = append)."""
    return (spark.readStream.format("iceberg")
            .option("streaming-skip-delete-snapshots", "true")
            .option("streaming-skip-overwrite-snapshots", "true")
            .option("streaming-max-files-per-micro-batch", config.BRONZE_MAX_FILES_PER_TRIGGER)
            .load(config.BRONZE_TABLE)
            .select(*BRONZE_COLUMNS))


def run_stream(run):
    def handler(df, batch_id):
        process_batch(df, run, stream_tag(run.spark, batch_id))
        maintain(run.spark, TABLES, batch_id)

    query = (read_bronze_stream(run.spark).writeStream.queryName(JOB_NAME).foreachBatch(handler)
             .option("checkpointLocation", config.SILVER_CHECKPOINT_PATH)
             .trigger(**stream_trigger(run.mode))
             .start())
    query.awaitTermination()


def scope_partitions(hour):
    """Backfill scope: hour H and H-1."""
    return [get_partition(hour - timedelta(hours=1)), get_partition(hour)]


def run_backfill(run):
    bronze = (run.spark.table(config.BRONZE_TABLE)
              .where(partition_predicate(scope_partitions(run.hour)))
              .select(*BRONZE_COLUMNS))
    process_batch(bronze, run, f"backfill-{run.hour:%Y%m%d%H}")


# ---------- entry point ----------

def check_output_contract(spark):
    """The tables must have exactly the columns this job writes, in order (INSERT * and append are positional)."""
    for table, expected in [(config.SILVER_TABLE, SILVER_COLUMNS),
                            (config.SILVER_QUARANTINE_TABLE, QUARANTINE_COLUMNS)]:
        actual = spark.table(table).columns
        if actual != expected:
            raise RuntimeError(f"{table} does not match the job's contract. "
                               f"Missing: {sorted(set(expected) - set(actual))}, "
                               f"unexpected: {sorted(set(actual) - set(expected))}, or different order")


def send_lineage():
    inputs = [gcs_dataset(config.BRONZE_TABLE_PATH)]
    outputs = [gcs_dataset(config.SILVER_TABLE_PATH), gcs_dataset(config.SILVER_QUARANTINE_PATH),
               kafka_dataset(config.SILVER_DLQ_TOPIC)]
    emit_lineage(JOB_NAME, inputs, outputs)


def main():
    args = parse_args()
    hour = run_hour(args)
    print(f"✔ {JOB_NAME} starting: mode={args.mode}, hour={hour:%Y-%m-%dT%H}:00Z, contract={contract.VERSION}")
    spark = create_spark(JOB_NAME)
    log_versions(spark)
    run_migrations(spark, MIGRATIONS)
    check_output_contract(spark)
    maintain(spark, TABLES)
    metrics = Metrics("bronze-to-silver", job=JOB_NAME, mode=args.mode, hour=f"{hour:%Y-%m-%dT%H}")
    run = Run(spark, args.mode, hour, metrics, scope_hours=2 if args.mode == "backfill" else None)
    started = time.time()
    try:
        send_lineage()  # sent at start: in continuous mode the job never reaches its end
        if args.mode == "backfill":
            run_backfill(run)
        else:
            run_stream(run)
        metrics.record_ms(f"{METRIC}.job_duration_ms", int((time.time() - started) * 1000))
    finally:
        metrics.close()  # force_flush: the process exits right after


if __name__ == "__main__":
    try:
        main()
        print(f"✔ {JOB_NAME} finished")
    except Exception as e:
        print(f"✖ {JOB_NAME} failed: {type(e).__name__}: {e}")
        sys.exit(1)
