import json
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField, StructType, TimestampType)

FIXTURES = Path(__file__).parent / "fixtures"

BRONZE_SCHEMA = StructType([
    StructField("case", StringType()),
    StructField("raw_json", StringType()),
    StructField("kafka_topic", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("ingested_at", TimestampType()),
    StructField("timestamp_kafka", TimestampType()),
    StructField("timestamp_bronze", TimestampType()),
    StructField("latency_to_kafka", LongType()),
    StructField("latency_to_bronze", LongType()),
    StructField("latency_processing_bronze", LongType()),
    *[StructField(name, StringType()) for name in ("year", "month", "day", "hour")],
])
TIMESTAMPS = {"ingested_at", "timestamp_kafka", "timestamp_bronze"}


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()


def load_rows():
    rows = []
    for line in (FIXTURES / "bronze_rows.jsonl").read_text().splitlines():
        row = json.loads(line)
        for name in TIMESTAMPS:
            row[name] = datetime.fromisoformat(row[name]) if row[name] else None
        rows.append(tuple(row.get(field.name) for field in BRONZE_SCHEMA.fields))
    return rows


@pytest.fixture(scope="session")
def bronze_df(spark):
    """Real bronze rows (2026-11-20) + synthetic cases: schema drift, invalid rows and a producer duplicate."""
    return spark.createDataFrame(load_rows(), BRONZE_SCHEMA)
