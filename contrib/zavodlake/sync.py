"""Upload lake parquet files to the public bucket mirror.

Implements the layout agreed in PLAN.md: each dataset's parquet goes to
``contrib/zlake/<dataset>/<version>/statements.parquet`` (immutable, long
TTL), and a ``parquet-latest.json`` pointer next to the dataset prefix is
the source of truth for the newest synced version (short TTL, invalidated
on write). The version is the run version recorded in the local
``index.json`` the pack was fetched from.
"""

import json
from datetime import UTC, datetime

import duckdb
from rigour.mime.types import JSON

from zavod.archive import INDEX_FILE, TTL_LONG, TTL_SHORT, dataset_resource_path
from zavod.archive.backend import get_archive_backend
from zavod.archive.cdn import invalidate_archive_cache
from zavod.logs import get_logger

from contrib.zavodlake.convert import dataset_parquet_path

log = get_logger(__name__)

ZLAKE_PREFIX = "contrib/zlake"
POINTER_FILE = "parquet-latest.json"
POINTER_SCHEMA = 1
PARQUET_MIME = "application/vnd.apache.parquet"


def dataset_version(dataset_name: str) -> str:
    """Return the run version the local pack/parquet was built from."""
    path = dataset_resource_path(dataset_name, INDEX_FILE)
    with open(path) as fh:
        data = json.load(fh)
    version = data.get("version")
    if not isinstance(version, str) or len(version) == 0:
        raise ValueError(f"No version in index file: {path}")
    return version


def sync_dataset(
    conn: duckdb.DuckDBPyConnection, dataset_name: str, force: bool = False
) -> str | None:
    """Upload a dataset's parquet and update its pointer file.

    Returns the synced version, or None if the remote pointer already
    references it. The parquet upload happens before the pointer write, so
    a reader following the pointer never sees a missing object.
    """
    parquet_path = dataset_parquet_path(dataset_name)
    version = dataset_version(dataset_name)
    backend = get_archive_backend()
    pointer_name = f"{ZLAKE_PREFIX}/{dataset_name}/{POINTER_FILE}"
    pointer_object = backend.get_object(pointer_name)
    if not force and pointer_object.exists():
        with pointer_object.open() as fh:
            remote = json.load(fh)
        if remote.get("version") == version:
            return None

    object_name = f"{ZLAKE_PREFIX}/{dataset_name}/{version}/statements.parquet"
    backend.get_object(object_name).publish(
        parquet_path, mime_type=PARQUET_MIME, ttl=TTL_LONG
    )

    row = conn.execute(
        "SELECT count(*) FROM read_parquet(?)", [parquet_path.as_posix()]
    ).fetchone()
    assert row is not None
    pointer = {
        "schema": POINTER_SCHEMA,
        "dataset": dataset_name,
        "version": version,
        "path": object_name,
        "rows": row[0],
        "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    pointer_path = parquet_path.parent / POINTER_FILE
    with open(pointer_path, "w") as fh:
        json.dump(pointer, fh, indent=2)
    pointer_object.publish(pointer_path, mime_type=JSON, ttl=TTL_SHORT)
    invalidate_archive_cache(pointer_name)
    return version
