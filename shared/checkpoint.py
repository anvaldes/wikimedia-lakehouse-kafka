from shared.gcs import upload_text, download_text


def load_checkpoint(bucket, path):
    return download_text(bucket, path)


def save_checkpoint(bucket, path, event_id):
    upload_text(bucket, path, event_id)
