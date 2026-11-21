"""Iceberg helpers shared by the jobs: partition-bounded insert-only MERGE, commit stats and table maintenance."""
from datetime import timedelta

from processing.common import config
from processing.common.columns import PARTITIONS, utc_now


# ---------- partition-bounded MERGE ----------

def partition_predicate(partitions, alias=None):
    """SQL predicate matching exactly these partitions (dicts with year/month/day/hour strings)."""
    prefix = f"{alias}." if alias else ""
    clauses = [" AND ".join(f"{prefix}{name} = '{p[name]}'" for name in PARTITIONS) for p in partitions]
    return " OR ".join(f"({clause})" for clause in clauses) or "FALSE"


def distinct_partitions(df):
    return [row.asDict() for row in df.select(*PARTITIONS).distinct().collect()]


def merge_insert_only(df, table, keys, partitions, view):
    """Inserts the rows whose key is not in the target yet (re-running inserts nothing).
    An insert-only MERGE runs as an anti-join + append, so the literal partition predicate is what
    keeps it from reading the whole target table. It also commits an 'append' snapshot, which keeps
    the table readable as a stream by the next layer."""
    on = " AND ".join(f"t.{key} = s.{key}" for key in [*PARTITIONS, *keys])
    df.createOrReplaceTempView(view)
    df.sparkSession.sql(f"""
        MERGE INTO {table} t
        USING {view} s
        ON {on} AND ({partition_predicate(partitions, "t")})
        WHEN NOT MATCHED THEN INSERT *
    """)


# ---------- commit stats ----------

def latest_snapshot(spark, table):
    spark.sql(f"REFRESH TABLE {table}")
    rows = spark.sql(f"SELECT snapshot_id, summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    return (rows[0]["snapshot_id"], rows[0]["summary"]) if rows else (None, {})


def added_records(spark, table, previous_snapshot_id):
    """Rows the last commit added (0 if nothing was committed). Assumes one writer per table, as in this project."""
    snapshot_id, summary = latest_snapshot(spark, table)
    if snapshot_id is None or snapshot_id == previous_snapshot_id:
        return 0
    return int(summary.get("added-records", 0))


# ---------- maintenance ----------

def expire_snapshots(spark, table, retention_hours=config.SNAPSHOT_RETENTION_HOURS):
    catalog, name = table.split(".", 1)
    older_than = utc_now() - timedelta(hours=retention_hours)
    row = spark.sql(f"""
        CALL {catalog}.system.expire_snapshots(
            table => '{name}',
            older_than => TIMESTAMP '{older_than:%Y-%m-%d %H:%M:%S}',
            retain_last => 1)
    """).first()
    print(f"✔ Snapshots expired on {table} (older than {retention_hours} h): {row.asDict() if row else {}}")


def set_metadata_properties(spark, table):
    """Keeps old metadata.json files from piling up in GCS (only commits if something changes)."""
    current = {row["key"]: row["value"] for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    missing = {key: value for key, value in config.METADATA_PROPERTIES.items() if current.get(key) != value}
    if missing:
        properties = ", ".join(f"'{key}' = '{value}'" for key, value in missing.items())
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({properties})")


def maintain(spark, tables, batch_id=None):
    """Run at job start (batch_id=None) and every MAINTENANCE_EVERY_BATCHES micro-batches.
    Best-effort: a failed run only logs; the next one retries."""
    if batch_id is not None and batch_id % config.MAINTENANCE_EVERY_BATCHES:
        return
    for table in tables:
        try:
            expire_snapshots(spark, table)
            set_metadata_properties(spark, table)
        except Exception as e:
            print(f"⚠ Maintenance failed on {table}: {type(e).__name__}: {e}")
