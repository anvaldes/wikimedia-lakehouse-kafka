#!/usr/bin/env bash
# Submits silver_to_gold as a Serverless for Apache Spark (Dataproc serverless) batch.
# Usage: processing/scripts/submit_silver_to_gold.sh YEAR MONTH DAY HOUR [stream|backfill|continuous]
#   continuous: 24/7, every 5 minutes recomputes the hours that received new silver rows (YEAR..HOUR only label the run)
#   backfill:   rewrites hours H and H-1 (anomalies: only H), H = YEAR MONTH DAY HOUR in UTC
set -euo pipefail

YEAR=$1 MONTH=$2 DAY=$3 HOUR=$4 MODE=${5:-stream}

# Max run time: continuous runs until the TTL stops it (one week); the others are a safety cap.
if [[ "$MODE" == "continuous" ]]; then TTL=7d; else TTL=4h; fi

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
BATCH_ID="silver-to-gold-${MODE}-$(printf '%04d%02d%02d%02d' $((10#$YEAR)) $((10#$MONTH)) $((10#$DAY)) $((10#$HOUR)))-$(date +%s)"

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
  -- --year "$YEAR" --month "$MONTH" --day "$DAY" --hour "$HOUR" --mode "$MODE"
