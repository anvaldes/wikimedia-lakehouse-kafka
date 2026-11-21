"""Contract v1 for the mediawiki/recentchange events stored in bronze.recentchange.raw_json.

Sources:
  - Official schema mediawiki/recentchange 1.0.1:
    https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/recentchange/latest.yaml
  - 21,367 real bronze events (2026-11-20, all with $schema /mediawiki/recentchange/1.0.0). They also carry
    fields EventStreams adds that the schema does not list (title_url, notify_url, meta.topic/partition/offset)
    and ingested_at, added by our producer.

raw_json is ALWAYS parsed with SCHEMA (never inferSchema) using PARSE_OPTIONS: PERMISSIVE + a corrupt-record
column, so a type mismatch marks the row instead of failing the job. A JSON key not listed here is schema
drift: it is logged and counted, never fatal (the official schema has additionalProperties: true).
"""
from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType

VERSION = "recentchange_v1"
CORRUPT_RECORD = "_corrupt_record"
PARSE_OPTIONS = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_RECORD}


def _struct(*fields):
    return StructType([StructField(name, data_type, nullable=True) for name, data_type in fields])


META = _struct(
    ("id", StringType()),            # unique event id -> silver row key
    ("dt", StringType()),            # ISO-8601 UTC event time -> event_ts (parsed in Spark)
    ("uri", StringType()),
    ("request_id", StringType()),
    ("domain", StringType()),
    ("stream", StringType()),
    ("topic", StringType()),         # Wikimedia's own Kafka topic/partition/offset (not ours)
    ("partition", IntegerType()),
    ("offset", LongType()),
)

OLD_NEW_INT = _struct(("old", IntegerType()), ("new", IntegerType()))
OLD_NEW_LONG = _struct(("old", LongType()), ("new", LongType()))

SCHEMA = _struct(
    ("$schema", StringType()),
    ("meta", META),
    ("id", LongType()),              # rcid. Null for ~6% of log events
    ("type", StringType()),          # edit | new | log | categorize | external
    ("namespace", IntegerType()),
    ("title", StringType()),
    ("title_url", StringType()),
    ("comment", StringType()),
    ("parsedcomment", StringType()),
    ("timestamp", LongType()),       # unix seconds
    ("user", StringType()),
    ("bot", BooleanType()),
    ("minor", BooleanType()),        # edit/new only
    ("patrolled", BooleanType()),    # only on wikis with patrolling
    ("length", OLD_NEW_INT),         # edit/new only
    ("revision", OLD_NEW_LONG),      # edit/new only
    ("server_url", StringType()),
    ("server_name", StringType()),
    ("server_script_path", StringType()),
    ("wiki", StringType()),
    ("notify_url", StringType()),
    ("log_id", LongType()),          # log_* only on type=log
    ("log_type", StringType()),
    ("log_action", StringType()),
    ("log_params", StringType()),    # object, array or string in the source: kept as JSON text
    ("log_action_comment", StringType()),
    ("ingested_at", StringType()),   # added by our producer
    (CORRUPT_RECORD, StringType()),
)

# Minimum required by the brief + what is always present in real events and needed to build silver:
#   $schema     -> schema_version (required by the official schema itself)
#   server_name -> project / language
#   namespace   -> namespace_name, and needed to interpret title
# Not required on purpose: id (null in ~6% of log events), title (null-ratio quality check instead).
REQUIRED_FIELDS = ["meta.id", "meta.dt", "wiki", "type", "$schema", "server_name", "namespace"]

TOP_LEVEL_KEYS = [name for name in SCHEMA.fieldNames() if name != CORRUPT_RECORD]
META_KEYS = META.fieldNames()
KNOWN_KEYS = TOP_LEVEL_KEYS + [f"meta.{key}" for key in META_KEYS]
