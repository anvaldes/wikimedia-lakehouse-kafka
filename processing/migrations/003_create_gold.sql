-- Gold layer: hourly aggregates of silver.recentchange, one table per question.
-- Partitions: event hour (UTC) as zero-padded strings, same convention as bronze and silver.
-- Each run rewrites the partitions of hours H and H-1 (anomalies_hourly: only H), so re-running is idempotent.
-- Every table also carries the pipeline timestamps (max over the events of the row) and latencies (average ms) of
-- the previous layers, plus its own timestamp_gold_<table>, latency_gold_<table> and latency_processing_<table>,
-- so the end-to-end latency can be followed table by table.
-- unique_editors / unique_pages are COUNT(DISTINCT): they can NOT be summed across hours or wikis
-- (the same editor or page would be counted more than once). Recompute them from silver instead.
CREATE NAMESPACE IF NOT EXISTS lakehouse.gold
LOCATION 'gs://wikimedia-lakehouse-kafka/gold';

CREATE TABLE IF NOT EXISTS lakehouse.gold.activity_hourly (
    hour_ts                   TIMESTAMP COMMENT 'date_trunc(hour, event_ts), UTC',
    wiki                      STRING,
    project                   STRING,
    language                  STRING,
    total_events              BIGINT    COMMENT 'All events of the wiki in the hour',
    edits                     BIGINT    COMMENT 'type = edit',
    new_pages                 BIGINT    COMMENT 'type = new',
    log_events                BIGINT    COMMENT 'type = log',
    categorize_events         BIGINT    COMMENT 'type = categorize',
    unique_editors            BIGINT    COMMENT 'COUNT(DISTINCT user). NOT summable across hours or wikis',
    unique_pages              BIGINT    COMMENT 'COUNT(DISTINCT title). NOT summable across hours or wikis',
    minor_edits               BIGINT    COMMENT 'Events with minor = true',
    anonymous_edits           BIGINT    COMMENT 'edit or new by an anonymous user (IP or temporary account)',
    anonymous_pct             DOUBLE    COMMENT 'anonymous_edits / (edits + new_pages), 0 if no edits',
    bytes_added               BIGINT    COMMENT 'Sum of length_delta where > 0',
    bytes_removed             BIGINT    COMMENT 'Sum of abs(length_delta) where < 0',
    net_bytes                 BIGINT    COMMENT 'Sum of length_delta',
    event_ts                  TIMESTAMP COMMENT 'From silver: max over the events of the row (latest meta.dt)',
    ingested_at               TIMESTAMP COMMENT 'From silver: max over the events of the row (latest received by the producer)',
    timestamp_kafka           TIMESTAMP COMMENT 'From silver: max over the events of the row',
    timestamp_bronze          TIMESTAMP COMMENT 'From silver: max over the events of the row',
    latency_to_kafka          BIGINT    COMMENT 'From silver. Average ms over the events of the row: timestamp_kafka - ingested_at',
    latency_to_bronze         BIGINT    COMMENT 'From silver. Average ms: timestamp_bronze - timestamp_kafka',
    latency_processing_bronze BIGINT    COMMENT 'From silver. Average ms Spark spent processing the bronze batch',
    timestamp_silver          TIMESTAMP COMMENT 'From silver: max over the events of the row (latest silver write)',
    latency_to_silver         BIGINT    COMMENT 'From silver. Average ms: timestamp_silver - timestamp_bronze',
    latency_processing_silver BIGINT    COMMENT 'From silver. Average ms Spark spent processing the silver batch',
    gold_processed_at         TIMESTAMP COMMENT 'When the gold batch that wrote the row started',
    timestamp_gold_activity_hourly TIMESTAMP COMMENT 'When the gold batch finished computing activity_hourly, right before writing it',
    latency_gold_activity_hourly BIGINT    COMMENT 'ms: timestamp_gold_activity_hourly - timestamp_silver (latest silver row of the group)',
    latency_processing_activity_hourly BIGINT    COMMENT 'ms: from gold_processed_at (batch start) to right before writing activity_hourly',
    year                      STRING,
    month                     STRING,
    day                       STRING,
    hour                      STRING
)
USING iceberg
PARTITIONED BY (year, month, day, hour)
LOCATION 'gs://wikimedia-lakehouse-kafka/gold/activity_hourly'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);

CREATE TABLE IF NOT EXISTS lakehouse.gold.bots_vs_humans_hourly (
    hour_ts                   TIMESTAMP COMMENT 'date_trunc(hour, event_ts), UTC',
    wiki                      STRING,
    project                   STRING,
    language                  STRING,
    editor_type               STRING    COMMENT 'bot | human_anonymous | human_registered',
    events                    BIGINT,
    edits                     BIGINT    COMMENT 'type = edit',
    new_pages                 BIGINT    COMMENT 'type = new',
    unique_editors            BIGINT    COMMENT 'COUNT(DISTINCT user). NOT summable across hours, wikis or editor types',
    net_bytes                 BIGINT    COMMENT 'Sum of length_delta',
    pct_of_wiki_events        DOUBLE    COMMENT 'events / events of the wiki in the hour. Sums to 1 per wiki and hour',
    event_ts                  TIMESTAMP COMMENT 'From silver: max over the events of the row (latest meta.dt)',
    ingested_at               TIMESTAMP COMMENT 'From silver: max over the events of the row (latest received by the producer)',
    timestamp_kafka           TIMESTAMP COMMENT 'From silver: max over the events of the row',
    timestamp_bronze          TIMESTAMP COMMENT 'From silver: max over the events of the row',
    latency_to_kafka          BIGINT    COMMENT 'From silver. Average ms over the events of the row: timestamp_kafka - ingested_at',
    latency_to_bronze         BIGINT    COMMENT 'From silver. Average ms: timestamp_bronze - timestamp_kafka',
    latency_processing_bronze BIGINT    COMMENT 'From silver. Average ms Spark spent processing the bronze batch',
    timestamp_silver          TIMESTAMP COMMENT 'From silver: max over the events of the row (latest silver write)',
    latency_to_silver         BIGINT    COMMENT 'From silver. Average ms: timestamp_silver - timestamp_bronze',
    latency_processing_silver BIGINT    COMMENT 'From silver. Average ms Spark spent processing the silver batch',
    gold_processed_at         TIMESTAMP COMMENT 'When the gold batch that wrote the row started',
    timestamp_gold_bots_vs_humans_hourly TIMESTAMP COMMENT 'When the gold batch finished computing bots_vs_humans_hourly, right before writing it',
    latency_gold_bots_vs_humans_hourly BIGINT    COMMENT 'ms: timestamp_gold_bots_vs_humans_hourly - timestamp_silver (latest silver row of the group)',
    latency_processing_bots_vs_humans_hourly BIGINT    COMMENT 'ms: from gold_processed_at (batch start) to right before writing bots_vs_humans_hourly',
    year                      STRING,
    month                     STRING,
    day                       STRING,
    hour                      STRING
)
USING iceberg
PARTITIONED BY (year, month, day, hour)
LOCATION 'gs://wikimedia-lakehouse-kafka/gold/bots_vs_humans_hourly'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);

CREATE TABLE IF NOT EXISTS lakehouse.gold.trending_pages_hourly (
    hour_ts                   TIMESTAMP COMMENT 'date_trunc(hour, event_ts), UTC',
    wiki                      STRING,
    project                   STRING,
    language                  STRING,
    title                     STRING    COMMENT 'Article (namespace Main) edited or created in the hour. Top 20 per wiki and hour',
    edits_in_hour             BIGINT    COMMENT 'edit + new events of the page in the hour',
    unique_editors            BIGINT    COMMENT 'COUNT(DISTINCT user). NOT summable across hours or pages',
    bot_edits                 BIGINT,
    human_edits               BIGINT    COMMENT 'edits_in_hour - bot_edits',
    net_bytes                 BIGINT    COMMENT 'Sum of length_delta',
    rank_in_wiki              INT       COMMENT 'dense_rank by edits_in_hour, unique_editors (desc). Ties share a rank',
    event_ts                  TIMESTAMP COMMENT 'From silver: max over the events of the row (latest meta.dt)',
    ingested_at               TIMESTAMP COMMENT 'From silver: max over the events of the row (latest received by the producer)',
    timestamp_kafka           TIMESTAMP COMMENT 'From silver: max over the events of the row',
    timestamp_bronze          TIMESTAMP COMMENT 'From silver: max over the events of the row',
    latency_to_kafka          BIGINT    COMMENT 'From silver. Average ms over the events of the row: timestamp_kafka - ingested_at',
    latency_to_bronze         BIGINT    COMMENT 'From silver. Average ms: timestamp_bronze - timestamp_kafka',
    latency_processing_bronze BIGINT    COMMENT 'From silver. Average ms Spark spent processing the bronze batch',
    timestamp_silver          TIMESTAMP COMMENT 'From silver: max over the events of the row (latest silver write)',
    latency_to_silver         BIGINT    COMMENT 'From silver. Average ms: timestamp_silver - timestamp_bronze',
    latency_processing_silver BIGINT    COMMENT 'From silver. Average ms Spark spent processing the silver batch',
    gold_processed_at         TIMESTAMP COMMENT 'When the gold batch that wrote the row started',
    timestamp_gold_trending_pages_hourly TIMESTAMP COMMENT 'When the gold batch finished computing trending_pages_hourly, right before writing it',
    latency_gold_trending_pages_hourly BIGINT    COMMENT 'ms: timestamp_gold_trending_pages_hourly - timestamp_silver (latest silver row of the group)',
    latency_processing_trending_pages_hourly BIGINT    COMMENT 'ms: from gold_processed_at (batch start) to right before writing trending_pages_hourly',
    year                      STRING,
    month                     STRING,
    day                       STRING,
    hour                      STRING
)
USING iceberg
PARTITIONED BY (year, month, day, hour)
LOCATION 'gs://wikimedia-lakehouse-kafka/gold/trending_pages_hourly'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);

CREATE TABLE IF NOT EXISTS lakehouse.gold.edit_size_hourly (
    hour_ts                   TIMESTAMP COMMENT 'date_trunc(hour, event_ts), UTC',
    wiki                      STRING,
    project                   STRING,
    language                  STRING,
    edit_size_bucket          STRING    COMMENT 'From silver, abs(length_delta): none 0, tiny <=50, small <=500, medium <=5000, large <=50000, huge',
    edits                     BIGINT    COMMENT 'type = edit only',
    avg_length_delta          DOUBLE    COMMENT 'Signed average: shows whether the bucket adds or removes content',
    pct_of_wiki_edits         DOUBLE    COMMENT 'edits / edits of the wiki in the hour. Sums to 1 per wiki and hour',
    event_ts                  TIMESTAMP COMMENT 'From silver: max over the events of the row (latest meta.dt)',
    ingested_at               TIMESTAMP COMMENT 'From silver: max over the events of the row (latest received by the producer)',
    timestamp_kafka           TIMESTAMP COMMENT 'From silver: max over the events of the row',
    timestamp_bronze          TIMESTAMP COMMENT 'From silver: max over the events of the row',
    latency_to_kafka          BIGINT    COMMENT 'From silver. Average ms over the events of the row: timestamp_kafka - ingested_at',
    latency_to_bronze         BIGINT    COMMENT 'From silver. Average ms: timestamp_bronze - timestamp_kafka',
    latency_processing_bronze BIGINT    COMMENT 'From silver. Average ms Spark spent processing the bronze batch',
    timestamp_silver          TIMESTAMP COMMENT 'From silver: max over the events of the row (latest silver write)',
    latency_to_silver         BIGINT    COMMENT 'From silver. Average ms: timestamp_silver - timestamp_bronze',
    latency_processing_silver BIGINT    COMMENT 'From silver. Average ms Spark spent processing the silver batch',
    gold_processed_at         TIMESTAMP COMMENT 'When the gold batch that wrote the row started',
    timestamp_gold_edit_size_hourly TIMESTAMP COMMENT 'When the gold batch finished computing edit_size_hourly, right before writing it',
    latency_gold_edit_size_hourly BIGINT    COMMENT 'ms: timestamp_gold_edit_size_hourly - timestamp_silver (latest silver row of the group)',
    latency_processing_edit_size_hourly BIGINT    COMMENT 'ms: from gold_processed_at (batch start) to right before writing edit_size_hourly',
    year                      STRING,
    month                     STRING,
    day                       STRING,
    hour                      STRING
)
USING iceberg
PARTITIONED BY (year, month, day, hour)
LOCATION 'gs://wikimedia-lakehouse-kafka/gold/edit_size_hourly'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);

-- Only pages flagged as anomalous in hour H (z_score > 3 and edits_in_hour >= 10).
-- Baseline: the same page (wiki + title) in H-24 .. H-1, hours without edits count as 0.
CREATE TABLE IF NOT EXISTS lakehouse.gold.anomalies_hourly (
    hour_ts                   TIMESTAMP COMMENT 'date_trunc(hour, event_ts), UTC: the hour H being scored',
    wiki                      STRING,
    project                   STRING,
    language                  STRING,
    title                     STRING,
    edits_in_hour             BIGINT    COMMENT 'edit + new events of the page in H',
    unique_editors            BIGINT    COMMENT 'COUNT(DISTINCT user) in H. NOT summable',
    baseline_avg_edits        DOUBLE    COMMENT 'Edits per hour in H-24 .. H-1, divided by 24 (empty hours = 0)',
    baseline_stddev           DOUBLE    COMMENT 'Population standard deviation over the same 24 hours (zeros included)',
    z_score                   DOUBLE    COMMENT '(edits_in_hour - baseline_avg_edits) / baseline_stddev (1 if stddev = 0)',
    is_anomaly                BOOLEAN   COMMENT 'z_score > 3 AND edits_in_hour >= 10. Always true: only anomalies are stored',
    event_ts                  TIMESTAMP COMMENT 'From silver: max over the events of the row (latest meta.dt)',
    ingested_at               TIMESTAMP COMMENT 'From silver: max over the events of the row (latest received by the producer)',
    timestamp_kafka           TIMESTAMP COMMENT 'From silver: max over the events of the row',
    timestamp_bronze          TIMESTAMP COMMENT 'From silver: max over the events of the row',
    latency_to_kafka          BIGINT    COMMENT 'From silver. Average ms over the events of the row: timestamp_kafka - ingested_at',
    latency_to_bronze         BIGINT    COMMENT 'From silver. Average ms: timestamp_bronze - timestamp_kafka',
    latency_processing_bronze BIGINT    COMMENT 'From silver. Average ms Spark spent processing the bronze batch',
    timestamp_silver          TIMESTAMP COMMENT 'From silver: max over the events of the row (latest silver write)',
    latency_to_silver         BIGINT    COMMENT 'From silver. Average ms: timestamp_silver - timestamp_bronze',
    latency_processing_silver BIGINT    COMMENT 'From silver. Average ms Spark spent processing the silver batch',
    gold_processed_at         TIMESTAMP COMMENT 'When the gold batch that wrote the row started',
    timestamp_gold_anomalies_hourly TIMESTAMP COMMENT 'When the gold batch finished computing anomalies_hourly, right before writing it',
    latency_gold_anomalies_hourly BIGINT    COMMENT 'ms: timestamp_gold_anomalies_hourly - timestamp_silver (latest silver row of the group)',
    latency_processing_anomalies_hourly BIGINT    COMMENT 'ms: from gold_processed_at (batch start) to right before writing anomalies_hourly',
    year                      STRING,
    month                     STRING,
    day                       STRING,
    hour                      STRING
)
USING iceberg
PARTITIONED BY (year, month, day, hour)
LOCATION 'gs://wikimedia-lakehouse-kafka/gold/anomalies_hourly'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
)
