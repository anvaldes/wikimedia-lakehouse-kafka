#!/usr/bin/env bash
# Submits silver_to_gold as a Serverless for Apache Spark (Dataproc serverless) batch.
# Usage: processing/scripts/submit_silver_to_gold.sh YEAR MONTH DAY HOUR
#   Rewrites the gold partitions of hour H and H-1 (anomalies: only H), H = YEAR MONTH DAY HOUR in UTC.
#   Run it a few minutes after the hour closes, e.g. at 11:10 UTC with H = 10.
set -euo pipefail

YEAR=$1 MONTH=$2 DAY=$3 HOUR=$4

# Safety cap: one hour of silver (+ 24 h for the anomaly baseline) takes minutes.
TTL=1h

# Cost cap: fixed at the minimum of 2 executors, never scales up.
SPARK_PROPERTIES="spark.executor.instances=2#spark.dynamicAllocation.minExecutors=2#spark.dynamicAllocation.maxExecutors=2"

PROJECT_ID=lab-experiments-514316
REGION=us-central1
BUCKET=wikimedia-lakehouse-kafka
SERVICE_ACCOUNT=portfolio-runtime@lab-experiments-514316.iam.gserviceaccount.com

# Versions must match the Dataproc runtime (Spark 3.5 / Scala 2.13 on runtime 2.2): check the job's first log line.
RUNTIME_VERSION=2.2
SCALA_VERSION=2.13
ICEBERG_VERSION=1.10.0

# No Kafka here: gold only reads and writes Iceberg.
PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_${SCALA_VERSION}:${ICEBERG_VERSION}"
PACKAGES+=",org.apache.iceberg:iceberg-gcp-bundle:${ICEBERG_VERSION}"

# 10# forces base 10: bash would read "08" / "09" as invalid octal numbers
BATCH_ID="silver-to-gold-$(printf '%04d%02d%02d%02d' $((10#$YEAR)) $((10#$MONTH)) $((10#$DAY)) $((10#$HOUR)))-$(date +%s)"

# ^#^ switches gcloud's list delimiter to '#' because spark.jars.packages contains commas.
gcloud dataproc batches submit pyspark "gs://${BUCKET}/code/jobs/silver_to_gold.py" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --batch="$BATCH_ID" \
  --version="$RUNTIME_VERSION" \
  --ttl="$TTL" \
  --subnet=default \
  --service-account="$SERVICE_ACCOUNT" \
  --py-files="gs://${BUCKET}/code/processing.zip" \
  --properties="^#^spark.jars.packages=${PACKAGES}#${SPARK_PROPERTIES}" \
  -- --year "$YEAR" --month "$MONTH" --day "$DAY" --hour "$HOUR"
