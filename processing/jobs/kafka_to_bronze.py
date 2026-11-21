"""Kafka -> Bronze (Iceberg).

Modes:
  continuous (real time): Structured Streaming, one micro-batch every CONTINUOUS_TRIGGER, never ends.
  stream     (scheduled): Structured Streaming + Trigger.AvailableNow: reads everything pending and exits.
  backfill   (replay):    batch read of the Kafka messages whose Kafka timestamp falls in --year/--month/--day/--hour
                          (only possible within the topic's 7-day retention).
continuous and stream share the Spark checkpoint, so you can switch between them, but never run both at once.
In continuous/stream the checkpoint decides which offsets to read; --year/--month/--day/--hour only label the run.

Idempotency: bronze is written with MERGE on (topic, partition, offset) -> re-running any batch inserts nothing new.
Errors go to GCS (dynamic overwrite per batch -> idempotent) and to a Kafka dead-letter topic (at-least-once).
"""
import sys
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from shared.validation import parse_and_validate
from processing.common import config
from processing.common.args import parse_args, run_hour
from processing.common.spark_session import create_spark, kafka_options, log_versions
from processing.common.migrations import run_migrations
from processing.common.iceberg import distinct_partitions, maintain, merge_insert_only
from processing.common.metrics import Metrics
from processing.common.lineage import emit_lineage, kafka_dataset, gcs_dataset

JOB_NAME = "kafka_to_bronze"
MIGRATIONS = ["001_create_bronze.sql"]
PARTITION_FORMATS = [("year", "yyyy"), ("month", "MM"), ("day", "dd"), ("hour", "HH")]
PARTITIONS = [name for name, _ in PARTITION_FORMATS]
BRONZE_COLUMNS = [
    "raw_json", "kafka_topic", "kafka_partition", "kafka_offset",
    "event_ts", "ingested_at", "timestamp_kafka", "timestamp_bronze",
    "latency_to_kafka", "latency_to_bronze", "latency_processing_bronze",
    *PARTITIONS,
]
ERROR_COLUMNS = [
    "error_reason", "raw_json", "kafka_topic", "kafka_partition", "kafka_offset",
    "timestamp_kafka", "failed_at", "job", "batch",
]
KAFKA_KEY = ["kafka_topic", "kafka_partition", "kafka_offset"]

# Same rules as the producer (shared/validation.py): empty value, invalid JSON, NaN/Infinity,
# not a JSON object, missing or wrongly-typed required fields, unparseable meta.dt.
validate_udf = F.udf(lambda raw: parse_and_validate(raw)[1], StringType())


# ---------- helpers ----------

def utc_now():
    return datetime.now(timezone.utc)


def ms_since(started):
    return int((utc_now() - started).total_seconds() * 1000)


def ms_between(end_col, start_col):
    """All latency columns share this definition: BIGINT milliseconds, end - start."""
    return F.expr(f"(unix_micros({end_col}) - unix_micros({start_col})) DIV 1000")


def json_ts(path):
    return F.try_to_timestamp(F.get_json_object(F.col("raw_json"), path))


def with_partitions(df, ts_col):
    """year/month/day/hour as zero-padded UTC strings (same convention as shared/partitions.py)."""
    return df.select("*", *[F.date_format(ts_col, fmt).alias(name) for name, fmt in PARTITION_FORMATS])


# ---------- transform ----------

def select_kafka_columns(kafka_df):
    return kafka_df.select(
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("timestamp_kafka"),
    )


def add_event_columns(df):
    df = df.withColumn("event_ts", F.coalesce(json_ts("$.meta.dt"), F.col("timestamp_kafka")))
    df = df.withColumn("ingested_at", json_ts("$.ingested_at"))
    return df.withColumn("latency_to_kafka", ms_between("timestamp_kafka", "ingested_at"))


def prepare(kafka_df):
    df = select_kafka_columns(kafka_df).withColumn("error_reason", validate_udf("raw_json"))
    return add_event_columns(df)


def stamp_bronze(df, started):
    """timestamp_bronze = processing finished; latency_processing_bronze = how long that processing took."""
    df = df.withColumn("timestamp_bronze", F.lit(utc_now()))
    df = df.withColumn("latency_to_bronze", ms_between("timestamp_bronze", "timestamp_kafka"))
    return df.withColumn("latency_processing_bronze", F.lit(ms_since(started)).cast("long"))


def bronze_rows(prepared, started):
    valid = prepared.filter(F.col("error_reason").isNull())
    return with_partitions(stamp_bronze(valid, started), "event_ts").select(*BRONZE_COLUMNS)


def error_rows(prepared, tag):
    invalid = prepared.filter(F.col("error_reason").isNotNull())
    invalid = invalid.withColumn("failed_at", F.lit(utc_now()))
    invalid = invalid.withColumn("job", F.lit(JOB_NAME)).withColumn("batch", F.lit(tag))
    return with_partitions(invalid.select(*ERROR_COLUMNS), "timestamp_kafka")


def count_rows(prepared):
    rows = prepared.groupBy(F.col("error_reason").isNull().alias("valid")).count().collect()
    counts = {row["valid"]: row["count"] for row in rows}
    return {"valid": counts.get(True, 0), "invalid": counts.get(False, 0)}


# ---------- write ----------

def write_errors_gcs(errors):
    """Dynamic overwrite on .../hour=HH/batch=<tag>/: re-running the same batch replaces its files."""
    (errors.write.mode("overwrite").option("partitionOverwriteMode", "dynamic")
        .partitionBy(*PARTITIONS, "batch").json(config.ERRORS_PATH))


def write_errors_kafka(errors):
    """At-least-once: consumers of the DLQ should dedupe on (kafka_topic, kafka_partition, kafka_offset)."""
    payload = errors.select(F.to_json(F.struct(*ERROR_COLUMNS)).alias("value"))
    payload.write.format("kafka").options(**kafka_options()).option("topic", config.BRONZE_DLQ_TOPIC).save()


def merge_into_bronze(rows):
    """Insert-only MERGE on (partition cols + topic, partition, offset): duplicates are skipped.
    Bounded to the batch's partitions: without it every batch read the whole table (61 s for 3k rows at 5M rows)."""
    rows = rows.persist()
    try:
        merge_insert_only(rows, config.BRONZE_TABLE, KAFKA_KEY, distinct_partitions(rows), "bronze_batch")
    finally:
        rows.unpersist()


def write_outputs(prepared, counts, started, tag):
    bronze = bronze_rows(prepared, started)
    if counts["invalid"]:
        errors = error_rows(prepared, tag)
        write_errors_gcs(errors)
        write_errors_kafka(errors)
    if counts["valid"]:
        merge_into_bronze(bronze)


# ---------- batch ----------

def report_batch(counts, started, tag, metrics):
    elapsed = ms_since(started)
    print(f"✔ Batch {tag}: {counts['valid']} valid, {counts['invalid']} invalid, {elapsed} ms")
    if counts["invalid"]:
        print(f"⚠ {counts['invalid']} invalid rows written to {config.ERRORS_PATH} and {config.BRONZE_DLQ_TOPIC}")
    metrics.add("kafka_to_bronze.rows_read", counts["valid"] + counts["invalid"])
    metrics.add("kafka_to_bronze.rows_valid", counts["valid"])
    metrics.add("kafka_to_bronze.rows_invalid", counts["invalid"])
    metrics.record_ms("kafka_to_bronze.batch_duration_ms", elapsed)


def process_batch(kafka_df, tag, metrics):
    started = utc_now()
    prepared = prepare(kafka_df).persist()
    try:
        counts = count_rows(prepared)
        if counts["valid"] or counts["invalid"]:
            write_outputs(prepared, counts, started, tag)
        report_batch(counts, started, tag, metrics)
    finally:
        prepared.unpersist()


# ---------- modes ----------

def read_stream(spark):
    return (spark.readStream.format("kafka").options(**kafka_options())
            .option("subscribe", ",".join(config.KAFKA_TOPICS))
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", config.MAX_OFFSETS_PER_TRIGGER)
            .load())


def stream_tag(spark, batch_id):
    """<query id>-<batch id>: unique even if the checkpoint is reset (a new checkpoint gets a new query id)."""
    return f"{spark.streams.active[0].id[:8]}-{batch_id}"


def stream_trigger(mode):
    """continuous: a micro-batch every CONTINUOUS_TRIGGER, forever. stream: everything pending, then exit."""
    return {"processingTime": config.CONTINUOUS_TRIGGER} if mode == "continuous" else {"availableNow": True}


def run_stream(spark, mode, metrics):
    def handler(df, batch_id):
        process_batch(df, stream_tag(spark, batch_id), metrics)
        maintain(spark, [config.BRONZE_TABLE], batch_id)  # expire snapshots: see config.SNAPSHOT_RETENTION_HOURS

    query = (read_stream(spark).writeStream.queryName(JOB_NAME).foreachBatch(handler)
             .option("checkpointLocation", config.CHECKPOINT_PATH)
             .trigger(**stream_trigger(mode))
             .start())
    query.awaitTermination()


def read_hour(spark, start):
    end = start + timedelta(hours=1)
    df = (spark.read.format("kafka").options(**kafka_options())
          .option("subscribe", ",".join(config.KAFKA_TOPICS))
          .option("startingTimestamp", str(int(start.timestamp() * 1000)))
          .option("startingOffsetsByTimestampStrategy", "latest")
          .option("endingOffsets", "latest")
          .load())
    return df.filter((F.col("timestamp") >= F.lit(start)) & (F.col("timestamp") < F.lit(end)))


def run_backfill(spark, hour, metrics):
    process_batch(read_hour(spark, hour), f"backfill-{hour:%Y%m%d%H}", metrics)


# ---------- entry point ----------

def send_lineage():
    inputs = [kafka_dataset(topic) for topic in config.KAFKA_TOPICS]
    outputs = [gcs_dataset(config.BRONZE_TABLE_PATH), gcs_dataset(config.ERRORS_PREFIX),
               kafka_dataset(config.BRONZE_DLQ_TOPIC)]
    emit_lineage(JOB_NAME, inputs, outputs)


def run(spark, args, hour, metrics):
    if args.mode == "backfill":
        run_backfill(spark, hour, metrics)
    else:
        run_stream(spark, args.mode, metrics)


def main():
    args = parse_args()
    hour = run_hour(args)
    print(f"✔ {JOB_NAME} starting: mode={args.mode}, hour={hour:%Y-%m-%dT%H}:00Z")
    spark = create_spark(JOB_NAME)
    log_versions(spark)
    run_migrations(spark, MIGRATIONS)
    maintain(spark, [config.BRONZE_TABLE])  # also unblocks a table whose metadata grew past BigLake's limit
    metrics = Metrics("kafka-to-bronze", mode=args.mode)
    started = time.time()
    try:
        send_lineage()  # sent at start: in continuous mode the job never reaches its end
        run(spark, args, hour, metrics)
        metrics.record_ms("kafka_to_bronze.job_duration_ms", int((time.time() - started) * 1000))
    finally:
        metrics.close()


if __name__ == "__main__":
    try:
        main()
        print(f"✔ {JOB_NAME} finished")
    except Exception as e:
        print(f"✖ {JOB_NAME} failed: {type(e).__name__}: {e}")
        sys.exit(1)
