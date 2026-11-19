import os
import sys
import time
import signal
from datetime import datetime, timezone

from requests.exceptions import RequestException

from shared.wikimedia_client import open_stream, read_events, is_canary
from shared.kafka_client import create_producer, publish
from shared.checkpoint import load_checkpoint, save_checkpoint
from shared.telemetry import init_metrics

print("✔ Producer is loading...")

TOPIC = os.environ.get("KAFKA_TOPIC", "recentchange")
BUCKET = os.environ.get("CHECKPOINT_BUCKET", "")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "checkpoints/producer/last_event_id.txt")
USER_AGENT = os.environ.get("WIKIMEDIA_USER_AGENT", "")
CHECKPOINT_EVERY_S = 30
MAX_FAILURES = 8

state = {"last_id": None, "failed": 0, "failures": 0, "published": 0, "stop": False}

provider, meter = init_metrics("wikimedia-producer")
events_received = meter.create_counter("events_received")
events_published = meter.create_counter("events_published")
reconnects = meter.create_counter("reconnects")
event_lag = meter.create_histogram("event_lag_seconds", unit="s")


def on_delivery(err, msg):
    if err:
        state["failed"] += 1


def get_lag(event):
    event_ts = datetime.fromisoformat(event["meta"]["dt"].replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - event_ts).total_seconds()


def checkpoint(producer):
    pending = producer.flush(10)
    if pending > 0 or state["failed"] > 0:
        raise RuntimeError(f"Publish failed: {state['failed']} failed, {pending} pending")
    if state["last_id"]:
        save_checkpoint(BUCKET, CHECKPOINT_PATH, state["last_id"])
        print(f"✔ Checkpoint saved, published so far: {state['published']}")


def handle_sigterm(signum, frame):
    print("⚠ SIGTERM received, shutting down...")
    state["stop"] = True


def run_stream(producer):
    response = open_stream(USER_AGENT, state["last_id"])
    response.raise_for_status()
    last_checkpoint = time.time()
    for event_id, event in read_events(response):
        if state["stop"]:
            break
        state["failures"] = 0
        events_received.add(1)
        if is_canary(event):
            continue
        publish(producer, TOPIC, event, on_delivery)
        state["last_id"] = event_id
        state["published"] += 1
        events_published.add(1)
        event_lag.record(get_lag(event))
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
        sys.exit(1)
    finally:
        provider.shutdown()