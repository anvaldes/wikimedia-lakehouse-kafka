from google.cloud import storage


def load_checkpoint(bucket, path):
    blob = storage.Client().bucket(bucket).blob(path)
    return blob.download_as_text() if blob.exists() else None


def save_checkpoint(bucket, path, event_id):
    storage.Client().bucket(bucket).blob(path).upload_from_string(event_id)