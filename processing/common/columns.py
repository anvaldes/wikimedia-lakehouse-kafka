"""Time, latency and partition column helpers shared by the jobs (same conventions as kafka_to_bronze)."""
from datetime import datetime, timezone

from pyspark.sql import functions as F

PARTITION_FORMATS = [("year", "yyyy"), ("month", "MM"), ("day", "dd"), ("hour", "HH")]
PARTITIONS = [name for name, _ in PARTITION_FORMATS]


def utc_now():
    return datetime.now(timezone.utc)


def ms_since(started):
    return int((utc_now() - started).total_seconds() * 1000)


def ms_between(end_col, start_col):
    """All latency columns share this definition: BIGINT milliseconds, end - start."""
    return F.expr(f"(unix_micros({end_col}) - unix_micros({start_col})) DIV 1000")


def with_partitions(df, ts_col):
    """year/month/day/hour as zero-padded UTC strings (same convention as shared/partitions.py)."""
    return df.select("*", *[F.date_format(ts_col, fmt).alias(name) for name, fmt in PARTITION_FORMATS])
