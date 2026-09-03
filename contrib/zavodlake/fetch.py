from pathlib import Path

from zavod.archive import INDEX_FILE, STATEMENTS_FILE, get_dataset_artifact
from zavod.logs import get_logger

log = get_logger(__name__)


def fetch_dataset(dataset_name: str) -> Path | None:
    """Ensure a dataset's metadata and statements are present in the local data path.

    Downloads ``index.json`` and ``statements.pack`` from the archive into
    ``data/datasets/<name>/`` unless they already exist locally. Returns the
    path to the pack file, or None if the archive has no statements for the
    dataset.
    """
    get_dataset_artifact(dataset_name, INDEX_FILE)
    path = get_dataset_artifact(dataset_name, STATEMENTS_FILE)
    if path.is_file() and path.stat().st_size == 0:
        # A valid pack always contains at least the header row; a zero-byte
        # file is a truncated download. Drop it and fetch again.
        log.warning("Zero-byte statements.pack, refetching", dataset=dataset_name)
        path.unlink()
        path = get_dataset_artifact(dataset_name, STATEMENTS_FILE)
        if path.is_file() and path.stat().st_size == 0:
            return None
    if not path.is_file():
        return None
    return path
