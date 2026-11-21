PROJECT_ID = "lab-experiments-514316"
REGION = "us-central1"
BUCKET = "wikimedia-lakehouse-kafka"

KAFKA_BOOTSTRAP = "bootstrap-q-f3vwmc9slirwmoy56bewqjl.a6424b16f.us-central1.managedkafka.s.cloud.goog:9092"
KAFKA_TOPICS = ["recentchange", "recentchange.backfill"]
BRONZE_DLQ_TOPIC = "recentchange.bronze.dlq"
MAX_OFFSETS_PER_TRIGGER = 500000
CONTINUOUS_TRIGGER = "1 minute"

CATALOG = "lakehouse"
BRONZE_TABLE = f"{CATALOG}.bronze.recentchange"
BRONZE_TABLE_PATH = "bronze/recentchange"

# Layout: gs://<bucket>/<layer>/<thing>-<service>/
CHECKPOINT_PATH = f"gs://{BUCKET}/bronze/checkpoints-kafka-to-bronze"
ERRORS_PREFIX = "bronze/errors-kafka-to-bronze"
ERRORS_PATH = f"gs://{BUCKET}/{ERRORS_PREFIX}"

# ---------- silver ----------
SILVER_TABLE = f"{CATALOG}.silver.recentchange"
SILVER_TABLE_PATH = "silver/recentchange"
SILVER_QUARANTINE_TABLE = f"{CATALOG}.silver.recentchange_quarantine"
SILVER_QUARANTINE_PATH = "silver/recentchange_quarantine"
SILVER_DLQ_TOPIC = "recentchange.silver.dlq"
SILVER_CHECKPOINT_PATH = f"gs://{BUCKET}/silver/checkpoints-bronze-to-silver"
# Rate limit when streaming from bronze (first run catches up on the whole bronze history).
BRONZE_MAX_FILES_PER_TRIGGER = 1000

# ---------- gold ----------
GOLD_NAMESPACE = f"{CATALOG}.gold"
GOLD_TABLE_NAMES = ["activity_hourly", "bots_vs_humans_hourly", "trending_pages_hourly",
                    "edit_size_hourly", "anomalies_hourly"]
GOLD_TABLES = {name: f"{GOLD_NAMESPACE}.{name}" for name in GOLD_TABLE_NAMES}
GOLD_TABLE_PATHS = {name: f"gold/{name}" for name in GOLD_TABLE_NAMES}
GOLD_CHECKPOINT_PATH = f"gs://{BUCKET}/gold/checkpoints-silver-to-gold"
# Each micro-batch recomputes whole hours (+ 24 h of anomaly baseline): every 5 minutes is fresh enough
# and keeps the work and the commits (5 tables) low.
GOLD_TRIGGER = "5 minutes"
SILVER_MAX_FILES_PER_TRIGGER = 2000

# ---------- Iceberg maintenance ----------
# BigLake's REST catalog rejects a metadata.json over ~1 MiB and every commit adds ~900 bytes (one snapshot).
# kafka_to_bronze died this way on 2026-11-20 after ~1150 one-minute commits ("Table metadata is too large").
# 6 h of one-minute commits is ~360 snapshots (~350 KB). A stream reading a table must not lag more than this.
SNAPSHOT_RETENTION_HOURS = 6
MAINTENANCE_EVERY_BATCHES = 60
METADATA_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
}
