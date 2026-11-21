import json
import os
import re
import shutil
import subprocess
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


# Spark 3.5 (= Dataproc runtime 2.2) runs on Java 8/11/17/21. From Java 23 on, Subject.getSubject() is
# disabled and Hadoop fails with "UnsupportedOperationException: getSubject is not supported".
SUPPORTED_JAVA = {8, 11, 17, 21}


def java_major_version():
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin" / "java") if java_home else shutil.which("java")
    try:
        output = subprocess.run([java, "-version"], capture_output=True, text=True).stderr
    except (OSError, TypeError):  # no java binary at all
        return None
    match = re.search(r'version "(\d+)(?:\.(\d+))?', output)
    if not match:
        return None
    major = int(match.group(1))
    return int(match.group(2)) if major == 1 else major  # "1.8.0_x" -> 8


def require_supported_java():
    version = java_major_version()
    if version not in SUPPORTED_JAVA:
        pytest.exit(
            f"Spark 3.5 needs Java {sorted(SUPPORTED_JAVA)}, found: {version or 'none'}. "
            "On macOS: brew install openjdk@17 && "
            "export JAVA_HOME=$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home",
            returncode=1,
        )


@pytest.fixture(scope="session")
def spark():
    require_supported_java()
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
