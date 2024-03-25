from pathlib import Path
from google.cloud.storage import Client  # type: ignore


def fetch_internal_data(key: str, path: Path) -> None:
    """Fetch non-published source data from the given `key` to `path`."""
    client = Client()
    bucket = client.get_bucket("internal-data.opensanctions.org")
    blob = bucket.blob(key)
    if path.exists():
        return
    if not blob.exists():
        raise FileNotFoundError(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(path)
