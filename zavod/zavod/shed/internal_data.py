from pathlib import Path
from typing import Generator
from google.cloud.storage import Client, Bucket  # type: ignore


def _get_internal_bucket() -> Bucket:
    return Client().get_bucket("internal-data.opensanctions.org")


def fetch_internal_data(key: str, path: Path) -> None:
    """Fetch non-published source data from the given `key` to `path`."""
    bucket = _get_internal_bucket()
    blob = bucket.blob(key)
    if path.exists():
        return
    if not blob.exists():
        raise FileNotFoundError(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(path)


def list_internal_data(prefix: str) -> Generator[str, None, None]:
    """Fetch non-published source data from the given `key` to `path`."""
    bucket = _get_internal_bucket()
    for blob in bucket.list_blobs(prefix=prefix):
        yield blob.name
