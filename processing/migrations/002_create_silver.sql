-- Silver layer: one row per Wikimedia event (meta_id), parsed with contract recentchange_v1,
-- deduplicated and enriched. Bronze columns are carried over for traceability.
-- Latency columns are all BIGINT milliseconds, computed as <later timestamp> - <earlier timestamp>.
-- Partitions: event hour (meta.dt, UTC) as zero-padded strings, same convention as bronze.
CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
LOCATION 'gs://wikimedia-lakehouse-kafka/silver';

CREATE TABLE IF NOT EXISTS lakehouse.silver.recentchange (
    meta_id                   STRING    COMMENT 'meta.id: unique event id. Row key (unique in the table)',
    event_ts                  TIMESTAMP COMMENT 'meta.dt parsed as UTC timestamp. Defines the partition',
    schema_version            STRING    COMMENT 'Producer schema ($schema), e.g. /mediawiki/recentchange/1.0.0',
    contract_version          STRING    COMMENT 'Contract used to parse raw_json, e.g. recentchange_v1',
    rc_id                     BIGINT    COMMENT 'id: recentchange id (rcid). Null for some log events',
    `type`                    STRING    COMMENT 'edit | new | log | categorize | external',
    wiki                      STRING    COMMENT 'Wiki database name, e.g. enwiki',
    server_name               STRING    COMMENT 'e.g. en.wikipedia.org',
    server_url                STRING,
    server_script_path        STRING,
    project                   STRING    COMMENT 'Derived from wiki (constant per wiki): wikipedia, wiktionary, wikidata, commons, meta...',
    language                  STRING    COMMENT 'Derived from wiki for language editions (en, es, zh-min-nan...), else null',
    namespace                 INT,
    namespace_name            STRING    COMMENT 'Canonical English name of the namespace, Other if wiki-specific',
    title                     STRING,
    title_url                 STRING,
    `user`                    STRING,
    is_anonymous              BOOLEAN   COMMENT 'IP address or temporary account (~YYYY-...)',
    bot                       BOOLEAN,
    minor                     BOOLEAN,
    patrolled                 BOOLEAN,
    `comment`                 STRING,
    rc_timestamp              TIMESTAMP COMMENT 'timestamp (unix seconds) as TIMESTAMP',
    length_old                INT,
    length_new                INT,
    length_delta              INT       COMMENT 'length_new - length_old (old = 0 for new pages). Null without length',
    edit_size_bucket          STRING    COMMENT 'abs(length_delta): none 0, tiny <=50, small <=500, medium <=5000, large <=50000, huge',
    revision_old              BIGINT,
    revision_new              BIGINT,
    log_id                    BIGINT,
    log_type                  STRING,
    log_action                STRING,
    log_params                STRING    COMMENT 'JSON text (object, array or string in the source)',
    log_action_comment        STRING,
    meta_uri                  STRING,
    meta_request_id           STRING,
    meta_domain               STRING,
    meta_stream               STRING,
    meta_topic                STRING    COMMENT 'Wikimedia Kafka topic (not ours)',
    meta_partition            INT,
    meta_offset               BIGINT,
    raw_json                  STRING    COMMENT 'From bronze: Kafka message value, unmodified',
    kafka_topic               STRING    COMMENT 'From bronze: source topic',
    kafka_partition           INT       COMMENT 'From bronze: source partition',
    kafka_offset              BIGINT    COMMENT 'From bronze: source offset of the copy kept after dedup',
    ingested_at               TIMESTAMP COMMENT 'From bronze: when the producer received the event',
    timestamp_kafka           TIMESTAMP COMMENT 'From bronze: Kafka record timestamp',
    timestamp_bronze          TIMESTAMP COMMENT 'From bronze: when the bronze batch finished processing',
    latency_to_kafka          BIGINT    COMMENT 'From bronze. ms: timestamp_kafka - ingested_at',
    latency_to_bronze         BIGINT    COMMENT 'From bronze. ms: timestamp_bronze - timestamp_kafka',
    latency_processing_bronze BIGINT    COMMENT 'From bronze. ms: time Spark spent processing the bronze batch',
    timestamp_silver          TIMESTAMP COMMENT 'When the silver batch finished processing, right before the MERGE',
    latency_to_silver         BIGINT    COMMENT 'ms: timestamp_silver - timestamp_bronze',
    latency_processing_silver BIGINT    COMMENT 'ms: time Spark spent processing the silver batch (parse + checks + transform)',
    year                      STRING,
    month                     STRING,
    day                       STRING,
    hour                      STRING
)
USING iceberg
PARTITIONED BY (year, month, day, hour)
LOCATION 'gs://wikimedia-lakehouse-kafka/silver/recentchange'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);

-- Rows of bronze that break contract recentchange_v1. Row key: (kafka_topic, kafka_partition, kafka_offset).
-- Partitions: same as the bronze row they come from.
CREATE TABLE IF NOT EXISTS lakehouse.silver.recentchange_quarantine (
    error_reason              STRING    COMMENT 'invalid_json | not_json_object | type_mismatch | missing_<field> | invalid_meta.dt | empty_value',
    raw_json                  STRING    COMMENT 'From bronze: Kafka message value, unmodified',
    kafka_topic               STRING,
    kafka_partition           INT,
    kafka_offset              BIGINT,
    timestamp_kafka           TIMESTAMP,
    run_hour                  STRING    COMMENT 'Hour the run was launched for (--year/--month/--day/--hour), YYYY-MM-DDTHH',
    failed_at                 TIMESTAMP,
    job                       STRING,
    batch                     STRING    COMMENT 'Micro-batch tag (<query id>-<batch id>) or backfill-YYYYMMDDHH',
    contract_version          STRING,
    year                      STRING,
    month                     STRING,
    day                       STRING,
    hour                      STRING
)
USING iceberg
PARTITIONED BY (year, month, day, hour)
LOCATION 'gs://wikimedia-lakehouse-kafka/silver/recentchange_quarantine'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
)
