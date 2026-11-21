# wikimedia-lakehouse-kafka

## Gold layer

`silver_to_gold` builds hourly aggregates of `silver.recentchange` into five Iceberg tables in `lakehouse.gold`, partitioned by `year/month/day/hour`, where the hour is the event hour in UTC.

It runs in one of three modes, like the other jobs:
- **`continuous`** (24/7): it reads silver as a stream. Every 5 minutes it recomputes, in full, the hours that received new silver rows, including any hour that gets late events. The open hour is provisional until its last events arrive.
- **`backfill`**: it rewrites hours H and H−1, except for `anomalies_hourly`, which it rewrites only for H. Use it for history that the stream can no longer see, because silver keeps only 6 h of snapshots.
- **`stream`**: it processes everything pending once and exits.

| Table | One row per | Answers |
|---|---|---|
| `activity_hourly` | hour × wiki | How much activity each wiki has, split by event type, with bytes added and removed |
| `bots_vs_humans_hourly` | hour × wiki × editor type | What share of the activity comes from bots, anonymous users and registered users |
| `trending_pages_hourly` | hour × wiki × article | Which 20 articles were edited the most |
| `edit_size_hourly` | hour × wiki × size bucket | How large the edits are (silver's `edit_size_bucket`) |
| `anomalies_hourly` | hour × wiki × page | Which pages had an edit spike: z-score > 3 against the previous 24 h, with at least 10 edits |

**Every gold table carries the pipeline's timing,** so you can follow the end-to-end latency table by table:
- The timestamps of the previous layers (`event_ts`, `ingested_at`, `timestamp_kafka`, `timestamp_bronze`, `timestamp_silver`) hold the **maximum** over the row's events, that is, the latest event in the row.
- Their latencies (`latency_to_kafka`, `latency_to_bronze`, `latency_processing_bronze`, `latency_to_silver`, `latency_processing_silver`) hold the **average** in ms over those events.
- Each table also has its own gold fields:
  - `timestamp_gold_<table>`: when gold finished computing the table, right before writing it.
  - `latency_gold_<table>`: `timestamp_gold_<table> − timestamp_silver`.
  - `latency_processing_<table>`: the ms from the start of the gold batch (`gold_processed_at`) to that moment.

**`unique_editors` and `unique_pages` can't be summed.** They are `COUNT(DISTINCT)` values: the same editor or page can show up in several hours, wikis or editor types, so summing the rows counts it more than once. To get distinct counts over a longer period or across several wikis, recompute them from `silver.recentchange`. Every other count (`total_events`, `edits`, `net_bytes`, …) can be summed.

Anomaly scores need 24 hours of silver data before the scored hour. During the first 24 hours of the history, the baseline is incomplete and the job flags more pages than it should.

```bash
processing/scripts/submit_silver_to_gold.sh $(date -u +"%Y %m %d %H") continuous   # 24/7
processing/scripts/submit_silver_to_gold.sh 2026 11 21 10 backfill                  # rewrite 09h-10h
```
