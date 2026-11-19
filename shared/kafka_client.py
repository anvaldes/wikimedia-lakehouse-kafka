import json
import time
import base64
from datetime import datetime, timezone

import google.auth
from google.auth.transport.requests import Request
from confluent_kafka import Producer

HEADER = json.dumps({"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"})
credentials, _ = google.auth.default()


def b64(text):
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("utf-8").rstrip("=")


def get_token(config):
    if not credentials.valid:
        credentials.refresh(Request())
    expiry = credentials.expiry.replace(tzinfo=timezone.utc)
    payload = json.dumps({"exp": expiry.timestamp(), "iss": "Google", "iat": time.time(), "scope": "kafka", "sub": credentials.service_account_email})
    return ".".join([b64(HEADER), b64(payload), b64(credentials.token)]), expiry.timestamp()


def create_producer(bootstrap):
    return Producer({
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "oauth_cb": get_token,
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "zstd",
    })


def publish(producer, topic, key, value, on_delivery):
    try:
        producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
    except BufferError:
        producer.poll(1)
        producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
    producer.poll(0)