import orjson
import shutil
from pathlib import Path
from functools import cache
from typing import Optional, Generator, BinaryIO
from zavod.logs import get_logger
from google.cloud.storage import Client, Bucket, Blob
from followthemoney.cli.util import MAX_LINE
from nomenklatura.statement import Statement

from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.collection import Collection

log = get_logger(__name__)
StatementGen = Generator[Statement, None, None]
BLOB_BASE = f"datasets/{settings.BACKFILL_VERSION}"
STATEMENTS_RESOURCE = "statements.json"
INDEX_RESOURCE = "index.json"
FTM_RESOURCE = "entities.ftm.json"


@cache
def get_backfill_bucket() -> Optional[Bucket]:
    if settings.BACKFILL_BUCKET is None:
        log.warn("No backfill bucket configured")
        return None
    client = Client()
    bucket = client.get_bucket(settings.BACKFILL_BUCKET)
    return bucket


def get_backfill_blob(dataset: Dataset, resource: str) -> Optional[Blob]:
    bucket = get_backfill_bucket()
    if bucket is None:
        return None
    blob_name = f"{BLOB_BASE}/{dataset.name}/{resource}"
    return bucket.get_blob(blob_name)


def backfill_resource(dataset: Dataset, resource: str, path: Path) -> Optional[Path]:
    blob = get_backfill_blob(dataset, resource)
    if blob is not None:
        log.info(
            "Backfilling dataset resource...",
            dataset=dataset.name,
            resource=resource,
            blob_name=blob.name,
        )
        blob.download_to_filename(path)
        return path
    return None


def dataset_path(dataset: Dataset) -> Path:
    return settings.DATASET_PATH.joinpath(dataset.name)


def dataset_resource_path(dataset: Dataset, resource: str) -> Path:
    path = dataset_path(dataset).joinpath(resource)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_dataset_resource(
    dataset: Dataset, resource: str, backfill: bool = True, force_backfill: bool = False
) -> Optional[Path]:
    path = dataset_resource_path(dataset, resource)
    if path.exists() and not force_backfill:
        return path
    if backfill or force_backfill:
        return backfill_resource(dataset, resource, path)
    return None
