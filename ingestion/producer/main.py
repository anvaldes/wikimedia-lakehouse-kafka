import os
import sys
import json
import time
import signal
from datetime import datetime, timezone

from requests.exceptions import RequestException

from shared.wikimedia_client import STREAM_URL, open_stream, read_events, is_canary
from shared.kafka_client import create_producer, publish
from shared.checkpoint import load_checkpoint, save_checkpoint
from shared.validation import parse_and_validate, parse_dt
from shared.error_sink import ErrorSink
from shared.telemetry import init_metrics

print("✔ Producer is loading...")

TOPIC = os.environ.get("KAFKA_TOPIC", "recentchange")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "recentchange.dlq")
BUCKET = os.environ.get("BUCKET", "")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "bronze/checkpoints-producer/last_event_id.txt")
ERRORS_PREFIX = os.environ.get("ERRORS_PREFIX", "bronze/errors-producer")
USER_AGENT = os.environ.get("WIKIMEDIA_USER_AGENT", "")
CHECKPOINT_EVERY_S = 30
MAX_FAILURES = 8

state = {"last_id": None, "failed": 0, "failures": 0, "published": 0, "stop": False}
errors = ErrorSink(BUCKET, ERRORS_PREFIX)

provider, meter = init_metrics("wikimedia-producer")
events_received = meter.create_counter("events_received")
events_published = meter.create_counter("events_published")
events_malformed = meter.create_counter("events_malformed")
events_dropped = meter.create_counter("events_dropped")
reconnects = meter.create_counter("reconnects")
event_lag = meter.create_histogram(
    "event_lag_ms",
    unit="ms",
    explicit_bucket_boundaries_advisory=[50, 100, 250, 500, 1000, 2000, 5000, 10000, 30000, 60000, 300000],
)


def on_delivery(err, msg):
    if err:
        state["failed"] += 1


def get_lag_ms(event):
    return (datetime.now(timezone.utc) - parse_dt(event["meta"]["dt"])).total_seconds() * 1000


def checkpoint(producer):
    pending = producer.flush(10)
    if pending > 0 or state["failed"] > 0:
        raise RuntimeError(f"Publish failed: {state['failed']} failed, {pending} pending")
    # Errors are persisted before advancing the checkpoint: if the write fails we crash
    # and replay from the previous checkpoint, so no error record is lost.
    errors.flush()
    if state["last_id"]:
        save_checkpoint(BUCKET, CHECKPOINT_PATH, state["last_id"])
        print(f"✔ Checkpoint saved, published so far: {state['published']}")


def send_to_dlq(producer, event_id, raw, reason):
    record = {
        "error": reason,
        "event_id": event_id,
        "raw": raw,
        "source": STREAM_URL,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    publish(producer, DLQ_TOPIC, None, json.dumps(record, ensure_ascii=False), on_delivery)
    errors.add("invalid_event", reason, event_id=event_id, raw=raw)
    events_malformed.add(1, {"reason": reason})


def handle_sigterm(signum, frame):
    print("⚠ SIGTERM received, shutting down...")
    state["stop"] = True


def run_stream(producer):
    response = open_stream(USER_AGENT, state["last_id"])
    response.raise_for_status()
    last_checkpoint = time.time()
    for event_id, raw in read_events(response):
        if state["stop"]:
            break
        state["failures"] = 0
        events_received.add(1)
        event, error = parse_and_validate(raw)
        if event is not None and is_canary(event):
            events_dropped.add(1, {"reason": "canary"})
        elif error:
            send_to_dlq(producer, event_id, raw, error)
        else:
            event["ingested_at"] = datetime.now(timezone.utc).isoformat()
            publish(producer, TOPIC, event["wiki"], json.dumps(event), on_delivery)
            state["published"] += 1
            events_published.add(1)
            event_lag.record(get_lag_ms(event))
        state["last_id"] = event_id
        if time.time() - last_checkpoint > CHECKPOINT_EVERY_S:
            checkpoint(producer)
            last_checkpoint = time.time()


def main():
    producer = create_producer(os.environ["KAFKA_BOOTSTRAP"])
    state["last_id"] = load_checkpoint(BUCKET, CHECKPOINT_PATH)
    print(f"✔ Starting from checkpoint: {state['last_id']}")
    while not state["stop"]:
        try:
            run_stream(producer)
        except RequestException as e:
            state["failures"] += 1
            reconnects.add(1)
            errors.add("stream_error", type(e).__name__, message=str(e), attempt=state["failures"])
            print(f"⚠ Stream error ({state['failures']}/{MAX_FAILURES}): {e}")
            if state["failures"] >= MAX_FAILURES:
                raise
            time.sleep(min(2 ** state["failures"], 60))
    checkpoint(producer)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        main()
        print("✔ Producer stopped cleanly")
    except Exception as e:
        print(f"✖ Fatal error: {e}")
        errors.add("fatal_error", type(e).__name__, message=str(e), last_event_id=state["last_id"])
        try:
            errors.flush()
        except Exception as flush_error:
            print(f"✖ Could not write error records: {flush_error}")
        sys.exit(1)
    finally:
        provider.shutdown()
