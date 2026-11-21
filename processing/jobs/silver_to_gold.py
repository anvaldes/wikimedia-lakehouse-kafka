"""Silver (Iceberg) -> Gold (Iceberg): hourly aggregates.

One hourly batch per run (no streaming): --year/--month/--day/--hour = H, in UTC. --mode is not used.
  activity_hourly, bots_vs_humans_hourly, trending_pages_hourly, edit_size_hourly: hours H and H-1
  anomalies_hourly: hour H only, scored against the same page in H-24 .. H-1
Reads only silver.recentchange, bounded to the partitions it needs (H-24 .. H).

Per run:
  1. read silver H-24 .. H (one read, reused by every table)
  2. build the 5 tables (one function each)
  3. quality checks on all of them: any critical failure -> nothing is written, the job fails
  4. replace the partitions of the run in each table (overwrite by partition filter)

Idempotency: each table's partitions for the run are replaced as a whole, even when the new result is empty
(e.g. an hour that no longer has anomalies), so re-running the same hour gives exactly the same tables.
Run it a few minutes after the hour closes (e.g. at HH:10 for H = HH-1): H-1 is recomputed to absorb late events.
"""
import sys
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import List

from pyspark.sql import Window
from pyspark.sql import functions as F

from processing.common import config, quality
from processing.common.args import parse_args, run_hour
from processing.common.columns import PARTITIONS, utc_now
from processing.common.iceberg import maintain, partition_predicate
from processing.common.lineage import emit_lineage, gcs_dataset
from processing.common.metrics import Metrics
from processing.common.migrations import run_migrations
from processing.common.spark_session import create_spark, log_versions
from shared.partitions import get_partition

JOB_NAME = "silver_to_gold"
METRIC = JOB_NAME  # metric name prefix
MIGRATIONS = ["003_create_gold.sql"]

# ---------- input contract: the silver columns this job uses ----------

SILVER_FIELDS = [
    "meta_id", "event_ts", "wiki", "project", "language", "type", "namespace_name", "title", "user",
    "bot", "is_anonymous", "minor", "length_delta", "edit_size_bucket", *PARTITIONS,
]

# ---------- rules ----------

SCOPE_HOURS = 2             # H and H-1
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
TAIL = ["gold_processed_at", *PARTITIONS]


@dataclass(frozen=True, eq=False)  # compared/hashed by identity: used as dict key
class GoldTable:
    name: str
    columns: List[str]
    keys: List[str]                 # granularity: unique per row
    count_columns: List[str]        # must never be negative
    lookback_hours: int             # partitions rewritten per run: H-(n-1) .. H

    @property
    def table(self):
        return config.GOLD_TABLES[self.name]


ACTIVITY = GoldTable(
    "activity_hourly",
    [*HEAD, "total_events", "edits", "new_pages", "log_events", "categorize_events", "unique_editors",
     "unique_pages", "minor_edits", "anonymous_edits", "anonymous_pct", "bytes_added", "bytes_removed",
     "net_bytes", *TAIL],
    keys=WIKI_HOUR,
    count_columns=["total_events", "edits", "new_pages", "log_events", "categorize_events", "unique_editors",
                   "unique_pages", "minor_edits", "anonymous_edits", "bytes_added", "bytes_removed"],
    lookback_hours=SCOPE_HOURS,
)
BOTS_VS_HUMANS = GoldTable(
    "bots_vs_humans_hourly",
    [*HEAD, "editor_type", "events", "edits", "new_pages", "unique_editors", "net_bytes", "pct_of_wiki_events", *TAIL],
    keys=[*WIKI_HOUR, "editor_type"],
    count_columns=["events", "edits", "new_pages", "unique_editors"],
    lookback_hours=SCOPE_HOURS,
)
TRENDING = GoldTable(
    "trending_pages_hourly",
    [*HEAD, "title", "edits_in_hour", "unique_editors", "bot_edits", "human_edits", "net_bytes", "rank_in_wiki", *TAIL],
    keys=[*WIKI_HOUR, "title"],
    count_columns=["edits_in_hour", "unique_editors", "bot_edits", "human_edits"],
    lookback_hours=SCOPE_HOURS,
)
EDIT_SIZE = GoldTable(
    "edit_size_hourly",
    [*HEAD, "edit_size_bucket", "edits", "avg_length_delta", "pct_of_wiki_edits", *TAIL],
    keys=[*WIKI_HOUR, "edit_size_bucket"],
    count_columns=["edits"],
    lookback_hours=SCOPE_HOURS,
)
ANOMALIES = GoldTable(
    "anomalies_hourly",
    [*HEAD, "title", "edits_in_hour", "unique_editors", "baseline_avg_edits", "baseline_stddev", "z_score",
     "is_anomaly", *TAIL],
    keys=[*WIKI_HOUR, "title"],
    count_columns=["edits_in_hour", "unique_editors"],
    lookback_hours=1,
)
TABLES = [ACTIVITY, BOTS_VS_HUMANS, TRENDING, EDIT_SIZE, ANOMALIES]


# ---------- helpers ----------

def hours_back(hour, count):
    """Partitions of hours H-(count-1) .. H, oldest first."""
    return [get_partition(hour - timedelta(hours=back)) for back in reversed(range(count))]


def is_type(*types):
    return F.col("type").isin(*types)


def count_if(condition):
    return F.sum(F.when(condition, 1).otherwise(0))


def sum_or_zero(column):
    return F.coalesce(F.sum(column), F.lit(0)).cast("long")


def share_of_wiki_hour(column):
    return F.col(column) / F.sum(column).over(Window.partitionBy(*WIKI_HOUR))


def finish(df, table, processed_at):
    return df.withColumn("gold_processed_at", F.lit(processed_at)).select(*table.columns)


def with_hour_ts(silver):
    return silver.withColumn("hour_ts", F.date_trunc("hour", "event_ts"))


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
    )
    return finish(df.withColumn("pct_of_wiki_edits", share_of_wiki_hour("edits")), EDIT_SIZE, processed_at)


def score_pages(window, hour):
    """Every page edited in H with its baseline and z-score (before keeping only the anomalies).
    window: silver rows of H-24 .. H. Baseline = edits per hour of the same page (wiki + title) in H-24 .. H-1,
    divided by 24 so hours without edits count as 0; stddev is the population stddev over those 24 values."""
    edits = with_hour_ts(window).filter(is_type(*EDIT_TYPES))
    page = ["wiki", "title"]
    current = edits.filter(F.col("hour_ts") == F.lit(hour)).groupBy(*GROUP_KEYS, "title").agg(
        F.count(F.lit(1)).alias("edits_in_hour"),
        F.countDistinct("user").alias("unique_editors"),
    )
    baseline_start = hour - timedelta(hours=BASELINE_HOURS)
    per_hour = (edits.filter((F.col("hour_ts") >= F.lit(baseline_start)) & (F.col("hour_ts") < F.lit(hour)))
                     .groupBy(*page, "hour_ts").agg(F.count(F.lit(1)).alias("n")))
    baseline = per_hour.groupBy(*page).agg(F.sum("n").alias("_sum"), F.sum(F.col("n") * F.col("n")).alias("_sum_sq"))
    scored = current.join(baseline, page, "left").fillna(0, subset=["_sum", "_sum_sq"])
    avg = F.col("_sum") / F.lit(BASELINE_HOURS)
    variance = F.col("_sum_sq") / F.lit(BASELINE_HOURS) - avg * avg
    scored = (scored.withColumn("baseline_avg_edits", avg)
                    .withColumn("baseline_stddev", F.sqrt(F.greatest(variance, F.lit(0.0)))))
    denominator = F.when(F.col("baseline_stddev") == 0, F.lit(1.0)).otherwise(F.col("baseline_stddev"))
    scored = scored.withColumn("z_score", (F.col("edits_in_hour") - F.col("baseline_avg_edits")) / denominator)
    return scored.withColumn("is_anomaly", (F.col("z_score") > Z_SCORE_THRESHOLD)
                             & (F.col("edits_in_hour") >= MIN_EDITS_FOR_ANOMALY))


def anomalies_hourly(window, hour, processed_at):
    return finish(score_pages(window, hour).filter(F.col("is_anomaly")), ANOMALIES, processed_at)


def build_tables(window, hour, processed_at):
    """{GoldTable: DataFrame}. window = silver H-24 .. H; every table but anomalies uses only H-1 .. H."""
    scope = window.where(partition_predicate(hours_back(hour, SCOPE_HOURS)))
    return {
        ACTIVITY: activity_hourly(scope, processed_at),
        BOTS_VS_HUMANS: bots_vs_humans_hourly(scope, processed_at),
        TRENDING: trending_pages_hourly(scope, processed_at),
        EDIT_SIZE: edit_size_hourly(scope, processed_at),
        ANOMALIES: anomalies_hourly(window, hour, processed_at),
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


def totals_match_silver(activity, scope):
    """sum(total_events) per hour in activity_hourly == silver rows per hour."""
    actual = activity.groupBy(*PARTITIONS).agg(F.sum("total_events").alias("value"))
    expected = scope.groupBy(*PARTITIONS).agg(F.count(F.lit(1)).alias("value"))
    return quality.counts_match("total_events_match_silver", actual, expected, PARTITIONS)


def run_quality(tables, scope):
    results = []
    for table, df in tables.items():
        checks = table_checks(table, df)
        if table is ACTIVITY:
            checks.append(totals_match_silver(df, scope))
        results += quality.prefixed(quality.run_checks(df, checks), table.name)
    return results


# ---------- read / write ----------

def read_silver(spark, hour):
    """Silver partitions H-24 .. H only (the anomaly baseline is the widest window)."""
    return (spark.table(config.SILVER_TABLE)
            .where(partition_predicate(hours_back(hour, BASELINE_HOURS + 1)))
            .select(*SILVER_FIELDS))


def write_table(table, df, hour):
    """Replaces exactly the run's partitions of the table (also when df is empty)."""
    partitions = hours_back(hour, table.lookback_hours)
    df.select(*table.columns).writeTo(table.table).overwrite(F.expr(partition_predicate(partitions)))


# ---------- run ----------

def run(spark, hour, metrics):
    processed_at = utc_now()
    window = read_silver(spark, hour).persist()
    tables = {}
    try:
        scope = window.where(partition_predicate(hours_back(hour, SCOPE_HOURS)))
        tables = {table: df.persist() for table, df in build_tables(window, hour, processed_at).items()}
        results = run_quality(tables, scope)
        quality.log_results(results, JOB_NAME, f"{hour:%Y%m%d%H}")
        for r in results:
            metrics.add(f"{METRIC}.checks_passed" if r.passed else f"{METRIC}.checks_failed", 1, check=r.name)
        quality.enforce(results)  # nothing has been written yet
        for table, df in tables.items():
            write_table(table, df, hour)
            rows = df.count()
            metrics.add(f"{METRIC}.rows_written", rows, table=table.name)
            print(f"✔ {table.name}: {rows} rows written for {', '.join(p['hour'] + 'h' for p in hours_back(hour, table.lookback_hours))}")
    finally:
        for df in tables.values():
            df.unpersist()
        window.unpersist()


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
    print(f"✔ {JOB_NAME} starting: hour={hour:%Y-%m-%dT%H}:00Z (writes {hours_back(hour, SCOPE_HOURS)[0]['hour']}h-{hour:%H}h)")
    spark = create_spark(JOB_NAME)
    log_versions(spark)
    run_migrations(spark, MIGRATIONS)
    check_output_contract(spark)
    maintain(spark, [table.table for table in TABLES])
    metrics = Metrics("silver-to-gold", job=JOB_NAME, hour=f"{hour:%Y-%m-%dT%H}")
    started = time.time()
    try:
        send_lineage()
        run(spark, hour, metrics)
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
