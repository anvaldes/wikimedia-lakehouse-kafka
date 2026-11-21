# wikimedia-lakehouse-kafka

## Gold layer

`silver_to_gold` builds hourly aggregates of `silver.recentchange` into five Iceberg tables in `lakehouse.gold`, partitioned by `year/month/day/hour`, where the hour is the event hour in UTC. Each run takes an hour H and rewrites hours H and H−1, so late events are absorbed. `anomalies_hourly` is the exception: it rewrites only H.

| Table | One row per | Answers |
|---|---|---|
| `activity_hourly` | hour × wiki | How much activity each wiki has, split by event type, with bytes added and removed |
| `bots_vs_humans_hourly` | hour × wiki × editor type | What share of the activity comes from bots, anonymous users and registered users |
| `trending_pages_hourly` | hour × wiki × article | Which 20 articles were edited the most |
| `edit_size_hourly` | hour × wiki × size bucket | How large the edits are (silver's `edit_size_bucket`) |
| `anomalies_hourly` | hour × wiki × page | Which pages had an edit spike: z-score > 3 against the previous 24 h, with at least 10 edits |

**`unique_editors` and `unique_pages` can't be summed.** They are `COUNT(DISTINCT)` values: the same editor or page can show up in several hours, wikis or editor types, so summing the rows counts it more than once. To get distinct counts over a longer period or across several wikis, recompute them from `silver.recentchange`. Every other count (`total_events`, `edits`, `net_bytes`, …) can be summed.

Run the job a few minutes after the hour closes:

```bash
processing/scripts/submit_silver_to_gold.sh 2026 11 21 10   # at ~11:10 UTC
```
