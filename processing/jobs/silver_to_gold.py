"""Silver (Iceberg) -> Gold (Iceberg): hourly aggregates.

Modes (same as the other jobs):
  continuous (real time): Structured Streaming over the silver table's new snapshots, a micro-batch every
                          GOLD_TRIGGER, never ends. Each micro-batch looks at which event hours its new silver rows
                          belong to and recomputes those whole hours (late events included, whatever hour).
                          A NEW checkpoint starts at silver's oldest retained snapshot (silver keeps 6 h of them):
                          older history must be loaded with backfill.
  stream     (scheduled): same source + Trigger.AvailableNow: processes everything new and exits.
  backfill   (replay):    hours H and H-1 (H = --year/--month/--day/--hour), anomalies only for H.
continuous and stream share the Spark checkpoint, so you can switch between them, but never run both at once.

Every run / micro-batch:
  1. read silver for the hours to recompute + the 24 h before them (anomaly baseline), one bounded read
  2. build the 5 tables (one function each)
  3. quality checks on all of them: any critical failure -> nothing is written, the job fails
  4. replace exactly those hours' partitions in each table (overwrite by partition filter)

Pipeline fields: every table carries the previous layers' timestamps (max over the row's events) and latencies
(average ms), plus its own timestamp_gold_<table> / latency_gold_<table> / latency_processing_<table>, stamped
right before writing that table (same meaning as timestamp_silver / latency_to_silver / latency_processing_silver).

Idempotency: partitions are replaced as a whole, even when the new result is empty (e.g. an hour that no longer
has anomalies), so recomputing an hour always gives the same rows. While an hour is still open its rows are
provisional (anomalies included); the micro-batch that brings its last events makes them final.
"""
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List

from pyspark.sql import Window
from pyspark.sql import functions as F

from processing.common import config, quality
from processing.common.args import parse_args, run_hour
from processing.common.columns import PARTITIONS, ms_between, ms_since, utc_now
from processing.common.iceberg import maintain, partition_predicate
from processing.common.lineage import emit_lineage, gcs_dataset
from processing.common.metrics import Metrics
from processing.common.migrations import run_migrations
from processing.common.spark_session import create_spark, log_versions
from processing.common.streaming import stream_tag, stream_trigger
from shared.partitions import get_partition

JOB_NAME = "silver_to_gold"
METRIC = JOB_NAME  # metric name prefix
MIGRATIONS = ["003_create_gold.sql"]

# ---------- input contract: the silver columns this job uses ----------

# Pipeline timestamps and latencies of the previous layers, carried into every gold table (aggregated per row):
# timestamps -> max (the latest event in the row), latencies -> average in ms over the row's events.
UPSTREAM_TIMESTAMPS = ["event_ts", "ingested_at", "timestamp_kafka", "timestamp_bronze", "timestamp_silver"]
UPSTREAM = [
    "event_ts", "ingested_at", "timestamp_kafka", "timestamp_bronze",
    "latency_to_kafka", "latency_to_bronze", "latency_processing_bronze",
    "timestamp_silver", "latency_to_silver", "latency_processing_silver",
]

SILVER_FIELDS = [
    "meta_id", "wiki", "project", "language", "type", "namespace_name", "title", "user",
    "bot", "is_anonymous", "minor", "length_delta", "edit_size_bucket", *UPSTREAM, *PARTITIONS,
]

# ---------- rules ----------

BASELINE_HOURS = 24         # anomalies: H-24 .. H-1
TOP_PAGES = 20
ARTICLE_NAMESPACE = "Main"  # silver's name for namespace 0 (articles)
EDIT_TYPES = ["edit", "new"]
Z_SCORE_THRESHOLD = 3
MIN_EDITS_FOR_ANOMALY = 10
PCT_TOLERANCE = 1e-6

# ---------- output contracts ----------

HOUR_KEYS = [*PARTITIONS, "hour_ts"]
WIKI_KEYS = ["wiki", "project", "language"]
GROUP_KEYS = [*HOUR_KEYS, *WIKI_KEYS]
WIKI_HOUR = [*PARTITIONS, "wiki"]  # one wiki in one hour (window partition for percentages)
HEAD = ["hour_ts", *WIKI_KEYS]


@dataclass(frozen=True, eq=False)  # compared/hashed by identity: used as dict key
class GoldTable:
    name: str
    body: List[str]                 # the table's own dimensions and metrics
    keys: List[str]                 # granularity: unique per row
    count_columns: List[str]        # must never be negative

    @property
    def table(self):
        return config.GOLD_TABLES[self.name]

    @property
    def gold_timestamp(self):
        return f"timestamp_gold_{self.name}"

    @property
    def gold_latency(self):
        return f"latency_gold_{self.name}"

    @property
    def gold_processing(self):
        return f"latency_processing_{self.name}"

    @property
    def built_columns(self):
        """What the transform function returns (checked by the quality checks)."""
        return [*HEAD, *self.body, *UPSTREAM, "gold_processed_at", *PARTITIONS]

    @property
    def columns(self):
        """What is written: + this table's gold timestamp and latencies, stamped right before writing it."""
        return [*HEAD, *self.body, *UPSTREAM, "gold_processed_at",
                self.gold_timestamp, self.gold_latency, self.gold_processing, *PARTITIONS]


ACTIVITY = GoldTable(
    "activity_hourly",
    ["total_events", "edits", "new_pages", "log_events", "categorize_events", "unique_editors",
     "unique_pages", "minor_edits", "anonymous_edits", "anonymous_pct", "bytes_added", "bytes_removed", "net_bytes"],
    keys=WIKI_HOUR,
    count_columns=["total_events", "edits", "new_pages", "log_events", "categorize_events", "unique_editors",
                   "unique_pages", "minor_edits", "anonymous_edits", "bytes_added", "bytes_removed"],
)
BOTS_VS_HUMANS = GoldTable(
    "bots_vs_humans_hourly",
    ["editor_type", "events", "edits", "new_pages", "unique_editors", "net_bytes", "pct_of_wiki_events"],
    keys=[*WIKI_HOUR, "editor_type"],
    count_columns=["events", "edits", "new_pages", "unique_editors"],
)
TRENDING = GoldTable(
    "trending_pages_hourly",
    ["title", "edits_in_hour", "unique_editors", "bot_edits", "human_edits", "net_bytes", "rank_in_wiki"],
    keys=[*WIKI_HOUR, "title"],
    count_columns=["edits_in_hour", "unique_editors", "bot_edits", "human_edits"],
)
EDIT_SIZE = GoldTable(
    "edit_size_hourly",
    ["edit_size_bucket", "edits", "avg_length_delta", "pct_of_wiki_edits"],
    keys=[*WIKI_HOUR, "edit_size_bucket"],
    count_columns=["edits"],
)
ANOMALIES = GoldTable(
    "anomalies_hourly",
    ["title", "edits_in_hour", "unique_editors", "baseline_avg_edits", "baseline_stddev", "z_score", "is_anomaly"],
    keys=[*WIKI_HOUR, "title"],
    count_columns=["edits_in_hour", "unique_editors"],
)
TABLES = [ACTIVITY, BOTS_VS_HUMANS, TRENDING, EDIT_SIZE, ANOMALIES]


# ---------- scope: which hours a run recomputes ----------

def hours_back(hour, count):
    """Hours H-(count-1) .. H, oldest first."""
    return [hour - timedelta(hours=back) for back in reversed(range(count))]


@dataclass(frozen=True)
class Scope:
    hours: List[datetime]           # aggregate tables are rewritten for these hours
    anomaly_hours: List[datetime]   # anomalies_hourly is rewritten for these hours

    def hours_of(self, table):
        return self.anomaly_hours if table is ANOMALIES else self.hours

    def partitions_of(self, table):
        return [get_partition(h) for h in self.hours_of(table)]

    def read_partitions(self):
        """Silver partitions needed: the hours themselves + the 24 h baseline of every anomaly hour."""
        needed = set(self.hours)
        for hour in self.anomaly_hours:
            needed.update(hours_back(hour, BASELINE_HOURS + 1))
        return [get_partition(h) for h in sorted(needed)]

    def label(self):
        return ",".join(f"{h:%Y-%m-%dT%H}" for h in self.hours)


def backfill_scope(hour):
    """H and H-1 for the aggregates (late events), only H for anomalies."""
    return Scope(hours=hours_back(hour, 2), anomaly_hours=[hour])


def stream_scope(hours):
    """A micro-batch recomputes every table for the hours its new silver rows belong to."""
    return Scope(hours=sorted(hours), anomaly_hours=sorted(hours))


def partition_hour(row):
    return datetime(int(row["year"]), int(row["month"]), int(row["day"]), int(row["hour"]), tzinfo=timezone.utc)


def affected_hours(micro_batch):
    return [partition_hour(row) for row in micro_batch.select(*PARTITIONS).distinct().collect()]


# ---------- helpers ----------

def is_type(*types):
    return F.col("type").isin(*types)


def count_if(condition):
    return F.sum(F.when(condition, 1).otherwise(0))


def sum_or_zero(column):
    return F.coalesce(F.sum(column), F.lit(0)).cast("long")


def share_of_wiki_hour(column):
    return F.col(column) / F.sum(column).over(Window.partitionBy(*WIKI_HOUR))


def upstream():
    """Aggregates of the previous layers' timestamps (max) and latencies (average ms), for any groupBy."""
    return [F.max(c).alias(c) if c in UPSTREAM_TIMESTAMPS else F.round(F.avg(c)).cast("long").alias(c)
            for c in UPSTREAM]


def finish(df, table, processed_at):
    return df.withColumn("gold_processed_at", F.lit(processed_at)).select(*table.built_columns)


def stamp_gold(df, table, started):
    """timestamp_gold_<table> = processing of this table finished, right before writing it (same meaning as
    timestamp_bronze / timestamp_silver). latency_gold_<table> = timestamp_gold - latest timestamp_silver of the row.
    latency_processing_<table> = ms from the start of the gold batch to that moment."""
    df = df.withColumn(table.gold_timestamp, F.lit(utc_now()))
    df = df.withColumn(table.gold_latency, ms_between(table.gold_timestamp, "timestamp_silver"))
    df = df.withColumn(table.gold_processing, F.lit(ms_since(started)).cast("long"))
    return df.select(*table.columns)


def with_hour_ts(silver):
    return silver.withColumn("hour_ts", F.date_trunc("hour", "event_ts"))


def in_hours(hours):
    return F.col("hour_ts").isin(*[F.lit(h) for h in hours])


# ---------- tables ----------

def activity_hourly(silver, processed_at):
    edits_and_new = F.col("edits") + F.col("new_pages")
    df = with_hour_ts(silver).groupBy(*GROUP_KEYS).agg(
        F.count(F.lit(1)).alias("total_events"),
        count_if(is_type("edit")).alias("edits"),
        count_if(is_type("new")).alias("new_pages"),
        count_if(is_type("log")).alias("log_events"),
        count_if(is_type("categorize")).alias("categorize_events"),
        F.countDistinct("user").alias("unique_editors"),
        F.countDistinct("title").alias("unique_pages"),
        count_if(F.col("minor")).alias("minor_edits"),
        count_if(is_type(*EDIT_TYPES) & F.col("is_anonymous")).alias("anonymous_edits"),
        sum_or_zero(F.when(F.col("length_delta") > 0, F.col("length_delta"))).alias("bytes_added"),
        sum_or_zero(F.when(F.col("length_delta") < 0, -F.col("length_delta"))).alias("bytes_removed"),
        sum_or_zero("length_delta").alias("net_bytes"),
        *upstream(),
    )
    df = df.withColumn("anonymous_pct", F.when(edits_and_new > 0, F.col("anonymous_edits") / edits_and_new)
                       .otherwise(F.lit(0.0)))
    return finish(df, ACTIVITY, processed_at)


def editor_type():
    return (F.when(F.col("bot"), "bot")
             .when(F.col("is_anonymous"), "human_anonymous")
             .otherwise("human_registered"))


def bots_vs_humans_hourly(silver, processed_at):
    df = with_hour_ts(silver).withColumn("editor_type", editor_type()).groupBy(*GROUP_KEYS, "editor_type").agg(
        F.count(F.lit(1)).alias("events"),
        count_if(is_type("edit")).alias("edits"),
        count_if(is_type("new")).alias("new_pages"),
        F.countDistinct("user").alias("unique_editors"),
        sum_or_zero("length_delta").alias("net_bytes"),
        *upstream(),
    )
    return finish(df.withColumn("pct_of_wiki_events", share_of_wiki_hour("events")), BOTS_VS_HUMANS, processed_at)


def trending_pages_hourly(silver, processed_at):
    """Top 20 articles per wiki and hour. rank_in_wiki is a dense_rank (ties share it), but at most 20 rows are
    kept per wiki and hour: ties at the cut are broken by title so the result is deterministic."""
    articles = with_hour_ts(silver).filter(is_type(*EDIT_TYPES) & (F.col("namespace_name") == ARTICLE_NAMESPACE))
    pages = articles.groupBy(*GROUP_KEYS, "title").agg(
        F.count(F.lit(1)).alias("edits_in_hour"),
        F.countDistinct("user").alias("unique_editors"),
        count_if(F.col("bot")).alias("bot_edits"),
        sum_or_zero("length_delta").alias("net_bytes"),
        *upstream(),
    ).withColumn("human_edits", F.col("edits_in_hour") - F.col("bot_edits"))
    ranking = [F.col("edits_in_hour").desc(), F.col("unique_editors").desc()]
    by_wiki_hour = Window.partitionBy(*WIKI_HOUR)
    pages = (pages.withColumn("rank_in_wiki", F.dense_rank().over(by_wiki_hour.orderBy(*ranking)))
                  .withColumn("_row", F.row_number().over(by_wiki_hour.orderBy(*ranking, F.col("title").asc())))
                  .filter(F.col("_row") <= TOP_PAGES))
    return finish(pages, TRENDING, processed_at)


def edit_size_hourly(silver, processed_at):
    df = with_hour_ts(silver).filter(is_type("edit")).groupBy(*GROUP_KEYS, "edit_size_bucket").agg(
        F.count(F.lit(1)).alias("edits"),
        F.avg("length_delta").alias("avg_length_delta"),
        *upstream(),
    )
    return finish(df.withColumn("pct_of_wiki_edits", share_of_wiki_hour("edits")), EDIT_SIZE, processed_at)


def score_pages(window, hours):
    """Every page edited in one of `hours`, with its baseline and z-score (before keeping only the anomalies).
    window: silver rows covering each hour H and its H-24 .. H-1. Baseline = edits per hour of the same page
    (wiki + title) in H-24 .. H-1, divided by 24 so hours without edits count as 0; stddev is the population
    stddev over those 24 values. All hours are scored in one pass (a range join on the page's hourly counts)."""
    edits = with_hour_ts(window).filter(is_type(*EDIT_TYPES))
    current = edits.filter(in_hours(hours)).groupBy(*GROUP_KEYS, "title").agg(
        F.count(F.lit(1)).alias("edits_in_hour"),
        F.countDistinct("user").alias("unique_editors"),
        *upstream(),  # of the page's events in the scored hour
    )
    history = (edits.groupBy("wiki", "title", "hour_ts").agg(F.count(F.lit(1)).alias("n"))
                    .select(F.col("wiki").alias("_wiki"), F.col("title").alias("_title"),
                            F.col("hour_ts").alias("_hour"), "n"))
    in_baseline = ((F.col("_wiki") == F.col("wiki")) & (F.col("_title") == F.col("title"))
                   & (F.col("_hour") >= F.col("hour_ts") - F.expr(f"INTERVAL {BASELINE_HOURS} HOURS"))
                   & (F.col("_hour") < F.col("hour_ts")))
    baseline = (current.select("wiki", "title", "hour_ts").join(history, in_baseline, "left")
                       .groupBy("wiki", "title", "hour_ts")
                       .agg(F.coalesce(F.sum("n"), F.lit(0)).alias("_sum"),
                            F.coalesce(F.sum(F.col("n") * F.col("n")), F.lit(0)).alias("_sum_sq")))
    scored = current.join(baseline, ["wiki", "title", "hour_ts"], "left").fillna(0, subset=["_sum", "_sum_sq"])
    avg = F.col("_sum") / F.lit(BASELINE_HOURS)
    variance = F.col("_sum_sq") / F.lit(BASELINE_HOURS) - avg * avg
    scored = (scored.withColumn("baseline_avg_edits", avg)
                    .withColumn("baseline_stddev", F.sqrt(F.greatest(variance, F.lit(0.0)))))
    denominator = F.when(F.col("baseline_stddev") == 0, F.lit(1.0)).otherwise(F.col("baseline_stddev"))
    scored = scored.withColumn("z_score", (F.col("edits_in_hour") - F.col("baseline_avg_edits")) / denominator)
    return scored.withColumn("is_anomaly", (F.col("z_score") > Z_SCORE_THRESHOLD)
                             & (F.col("edits_in_hour") >= MIN_EDITS_FOR_ANOMALY))


def anomalies_hourly(window, hours, processed_at):
    return finish(score_pages(window, hours).filter(F.col("is_anomaly")), ANOMALIES, processed_at)


def scope_rows(window, scope):
    """Silver rows of the hours whose aggregates are recomputed."""
    return window.where(partition_predicate([get_partition(h) for h in scope.hours]))


def build_tables(window, scope, processed_at):
    """{GoldTable: DataFrame}. window = silver rows of scope.read_partitions()."""
    silver = scope_rows(window, scope)
    return {
        ACTIVITY: activity_hourly(silver, processed_at),
        BOTS_VS_HUMANS: bots_vs_humans_hourly(silver, processed_at),
        TRENDING: trending_pages_hourly(silver, processed_at),
        EDIT_SIZE: edit_size_hourly(silver, processed_at),
        ANOMALIES: anomalies_hourly(window, scope.anomaly_hours, processed_at),
    }


# ---------- quality ----------

def table_checks(table, df):
    checks = [quality.no_negatives(table.count_columns), quality.no_duplicate_keys(df, table.keys)]
    if table is BOTS_VS_HUMANS:
        checks.append(quality.group_sums_to(df, WIKI_HOUR, "pct_of_wiki_events", 1.0, PCT_TOLERANCE))
    if table is EDIT_SIZE:
        checks.append(quality.group_sums_to(df, WIKI_HOUR, "pct_of_wiki_edits", 1.0, PCT_TOLERANCE))
    if table is TRENDING:
        checks.append(quality.max_rows_per_group(df, WIKI_HOUR, TOP_PAGES))
    return checks


def totals_match_silver(activity, silver):
    """sum(total_events) per hour in activity_hourly == silver rows per hour."""
    actual = activity.groupBy(*PARTITIONS).agg(F.sum("total_events").alias("value"))
    expected = silver.groupBy(*PARTITIONS).agg(F.count(F.lit(1)).alias("value"))
    return quality.counts_match("total_events_match_silver", actual, expected, PARTITIONS)


def run_quality(tables, silver):
    """silver: the silver rows of the recomputed hours (scope_rows)."""
    results = []
    for table, df in tables.items():
        checks = table_checks(table, df)
        if table is ACTIVITY:
            checks.append(totals_match_silver(df, silver))
        results += quality.prefixed(quality.run_checks(df, checks), table.name)
    return results


# ---------- read / write ----------

def read_silver(spark, scope):
    """Only the silver partitions the scope needs (never the whole table)."""
    return (spark.table(config.SILVER_TABLE)
            .where(partition_predicate(scope.read_partitions()))
            .select(*SILVER_FIELDS))


def write_table(table, df, scope, started):
    """Stamps the table's gold timestamp/latencies and replaces exactly the scope's partitions (also when df is empty)."""
    stamped = stamp_gold(df, table, started)
    stamped.writeTo(table.table).overwrite(F.expr(partition_predicate(scope.partitions_of(table))))


# ---------- batch ----------

def process(spark, scope, tag, metrics):
    started = utc_now()
    window = read_silver(spark, scope).persist()
    tables = {}
    try:
        tables = {table: df.persist() for table, df in build_tables(window, scope, started).items()}
        results = run_quality(tables, scope_rows(window, scope))
        quality.log_results(results, JOB_NAME, tag)
        for r in results:
            metrics.add(f"{METRIC}.checks_passed" if r.passed else f"{METRIC}.checks_failed", 1, check=r.name)
        quality.enforce(results)  # nothing has been written yet
        written = {}
        for table, df in tables.items():
            write_table(table, df, scope, started)
            written[table.name] = df.count()
            metrics.add(f"{METRIC}.rows_written", written[table.name], table=table.name)
        elapsed = int((utc_now() - started).total_seconds() * 1000)
        metrics.record_ms(f"{METRIC}.batch_duration_ms", elapsed)
        print(f"✔ Batch {tag}: hours {scope.label()} -> {written}, {elapsed} ms")
    finally:
        for df in tables.values():
            df.unpersist()
        window.unpersist()


# ---------- modes ----------

def read_silver_stream(spark):
    """Only the partition columns: a micro-batch just tells which hours received new silver rows.
    Silver is written with insert-only MERGE (= append snapshots)."""
    return (spark.readStream.format("iceberg")
            .option("streaming-skip-delete-snapshots", "true")
            .option("streaming-skip-overwrite-snapshots", "true")
            .option("streaming-max-files-per-micro-batch", config.SILVER_MAX_FILES_PER_TRIGGER)
            .load(config.SILVER_TABLE)
            .select(*PARTITIONS))


def run_stream(spark, mode, metrics):
    tables = [table.table for table in TABLES]

    def handler(df, batch_id):
        hours = affected_hours(df)
        tag = stream_tag(spark, batch_id)
        if hours:
            process(spark, stream_scope(hours), tag, metrics)
        else:
            print(f"✔ Batch {tag}: no new rows")
        maintain(spark, tables, batch_id)

    query = (read_silver_stream(spark).writeStream.queryName(JOB_NAME).foreachBatch(handler)
             .option("checkpointLocation", config.GOLD_CHECKPOINT_PATH)
             .trigger(**stream_trigger(mode, config.GOLD_TRIGGER))
             .start())
    query.awaitTermination()


def run_backfill(spark, hour, metrics):
    process(spark, backfill_scope(hour), f"backfill-{hour:%Y%m%d%H}", metrics)


# ---------- entry point ----------

def check_output_contract(spark):
    """The tables must have exactly the columns this job writes, in order."""
    for table in TABLES:
        actual = spark.table(table.table).columns
        if actual != table.columns:
            raise RuntimeError(f"{table.table} does not match the job's contract. "
                               f"Missing: {sorted(set(table.columns) - set(actual))}, "
                               f"unexpected: {sorted(set(actual) - set(table.columns))}, or different order")


def send_lineage():
    inputs = [gcs_dataset(config.SILVER_TABLE_PATH)]
    outputs = [gcs_dataset(config.GOLD_TABLE_PATHS[table.name]) for table in TABLES]
    emit_lineage(JOB_NAME, inputs, outputs)


def main():
    args = parse_args()
    hour = run_hour(args)
    print(f"✔ {JOB_NAME} starting: mode={args.mode}, hour={hour:%Y-%m-%dT%H}:00Z")
    spark = create_spark(JOB_NAME)
    log_versions(spark)
    run_migrations(spark, MIGRATIONS)
    check_output_contract(spark)
    maintain(spark, [table.table for table in TABLES])
    metrics = Metrics("silver-to-gold", job=JOB_NAME, mode=args.mode, hour=f"{hour:%Y-%m-%dT%H}")
    started = time.time()
    try:
        send_lineage()  # sent at start: in continuous mode the job never reaches its end
        if args.mode == "backfill":
            run_backfill(spark, hour, metrics)
        else:
            run_stream(spark, args.mode, metrics)
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
