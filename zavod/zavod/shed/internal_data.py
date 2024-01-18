from pathlib import Path
from google.cloud.storage import Client


def fetch_internal_data(key: str, path: Path) -> None:
    """Fetch non-published source data from the given `key` to `path`."""
    client = Client()
    bucket = client.get_bucket("internal-data.opensanctions.org")
    blob = bucket.blob(key)
    blob.download_to_filename(path)
