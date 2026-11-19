from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

# Exponential backoff (1s -> 60s) on transient errors (429, 5xx, connection), up to 120s total.
# Uploads are not retried by default unless if_generation_match is set, so we pass it explicitly.
RETRY = DEFAULT_RETRY

_client = None


def client():
    global _client
    if _client is None:
        _client = storage.Client()
    return _client


def upload_text(bucket, path, text, content_type="text/plain"):
    client().bucket(bucket).blob(path).upload_from_string(text, content_type=content_type, retry=RETRY)


def download_text(bucket, path):
    blob = client().bucket(bucket).blob(path)
    return blob.download_as_text(retry=RETRY) if blob.exists(retry=RETRY) else None
