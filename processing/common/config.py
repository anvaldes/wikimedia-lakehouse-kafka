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
