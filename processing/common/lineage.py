"""Dataplex data lineage via the Data Lineage API's OpenLineage endpoint (best-effort: never fails the job)."""
import uuid
from datetime import datetime, timezone

from processing.common import config

LINEAGE_URL = "https://datalineage.googleapis.com/v1/projects/{project}/locations/{location}:processOpenLineageRunEvent"
PRODUCER = "https://github.com/anvaldes/wikimedia-lakehouse-kafka"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
JOB_NAMESPACE = "wikimedia-lakehouse-kafka"


def dataset(namespace, name):
    return {"namespace": namespace, "name": name}


def kafka_dataset(topic):
    return dataset(f"kafka://{config.KAFKA_BOOTSTRAP}", topic)


def gcs_dataset(path):
    return dataset(f"gs://{config.BUCKET}", path)


def build_event(job_name, inputs, outputs):
    return {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": {"runId": str(uuid.uuid4())},
        "job": {"namespace": JOB_NAMESPACE, "name": job_name},
        "inputs": inputs,
        "outputs": outputs,
    }


def post_event(event):
    import google.auth
    from google.auth.transport.requests import AuthorizedSession
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    url = LINEAGE_URL.format(project=config.PROJECT_ID, location=config.REGION)
    AuthorizedSession(credentials).post(url, json=event, timeout=30).raise_for_status()


def emit_lineage(job_name, inputs, outputs):
    try:
        post_event(build_event(job_name, inputs, outputs))
        print(f"✔ Lineage sent to Dataplex: {len(inputs)} inputs -> {len(outputs)} outputs")
    except Exception as e:
        print(f"⚠ Lineage not sent: {type(e).__name__}: {e}")
