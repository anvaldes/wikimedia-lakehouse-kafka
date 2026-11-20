-- Bronze layer: one row per Kafka message, raw payload kept as-is (schema-on-read).
-- Latency columns are all BIGINT milliseconds, computed as <later timestamp> - <earlier timestamp>.
CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze
LOCATION 'gs://wikimedia-lakehouse-kafka/bronze';

CREATE TABLE IF NOT EXISTS lakehouse.bronze.recentchange (
    raw_json                  STRING    COMMENT 'Kafka message value, unmodified',
    kafka_topic               STRING    COMMENT 'Source topic',
    kafka_partition           INT       COMMENT 'Source partition',
    kafka_offset              BIGINT    COMMENT 'Source offset; (topic, partition, offset) is the row key',
    event_ts                  TIMESTAMP COMMENT 'meta.dt, or timestamp_kafka if meta.dt cannot be parsed',
    ingested_at               TIMESTAMP COMMENT 'When the producer received the event (null for old messages)',
    timestamp_kafka           TIMESTAMP COMMENT 'Kafka record timestamp (LogAppendTime: broker write time)',
    timestamp_bronze          TIMESTAMP COMMENT 'When the batch finished processing, right before writing to bronze',
    latency_to_kafka          BIGINT    COMMENT 'ms: timestamp_kafka - ingested_at',
    latency_to_bronze         BIGINT    COMMENT 'ms: timestamp_bronze - timestamp_kafka (includes waiting for the hourly run)',
    latency_processing_bronze BIGINT    COMMENT 'ms: time Spark spent processing the batch (read + validate + transform)',
    year                      STRING,
    month                     STRING,
    day                       STRING,
    hour                      STRING
)
USING iceberg
PARTITIONED BY (year, month, day, hour)
LOCATION 'gs://wikimedia-lakehouse-kafka/bronze/recentchange'
TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet')
