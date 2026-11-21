"""Structured Streaming and dead-letter helpers shared by the jobs."""
from pyspark.sql import functions as F

from processing.common import config
from processing.common.spark_session import kafka_options


def stream_trigger(mode):
    """continuous: a micro-batch every CONTINUOUS_TRIGGER, forever. stream: everything pending, then exit."""
    return {"processingTime": config.CONTINUOUS_TRIGGER} if mode == "continuous" else {"availableNow": True}


def stream_tag(spark, batch_id):
    """<query id>-<batch id>: unique even if the checkpoint is reset (a new checkpoint gets a new query id)."""
    return f"{spark.streams.active[0].id[:8]}-{batch_id}"


def write_kafka(df, topic, columns):
    """One JSON message per row. At-least-once: consumers should dedupe on the row key."""
    payload = df.select(F.to_json(F.struct(*columns)).alias("value"))
    payload.write.format("kafka").options(**kafka_options()).option("topic", topic).save()
