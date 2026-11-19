import json
import uuid
from datetime import datetime, timezone

from shared.gcs import upload_text
from shared.partitions import get_partition, build_path


class ErrorSink:
    """Buffers error records in memory and writes them as one JSONL file per flush to
    gs://<bucket>/<layer>/<name>/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>-<uuid>.jsonl"""

    def __init__(self, bucket, prefix):
        self.bucket = bucket
        self.prefix = prefix
        self.buffer = []

    def add(self, kind, reason, **fields):
        self.buffer.append({
            "kind": kind,
            "reason": reason,
            "logged_at": datetime.now(timezone.utc).isoformat(),
            **fields,
        })

    def flush(self):
        if not self.buffer:
            return
        if not self.bucket:
            print(f"⚠ BUCKET not set, discarding {len(self.buffer)} error records")
            self.buffer = []
            return
        now = datetime.now(timezone.utc)
        path = f"{build_path(self.prefix, get_partition(now))}/{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.jsonl"
        body = "\n".join(json.dumps(r, ensure_ascii=False) for r in self.buffer) + "\n"
        upload_text(self.bucket, path, body, content_type="application/x-ndjson")
        print(f"✔ {len(self.buffer)} error records written to gs://{self.bucket}/{path}")
        self.buffer = []
