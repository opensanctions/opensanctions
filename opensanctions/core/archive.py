from pathlib import Path
from functools import cache
from typing import Optional
from zavod.logs import get_logger
from google.cloud.storage import Client, Bucket
from followthemoney.cli.util import path_entities

from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity

log = get_logger(__name__)
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


def dataset_path(dataset: Dataset) -> Path:
    return settings.DATASET_PATH.joinpath(dataset.name)


def dataset_resource_path(dataset: Dataset, resource: str) -> Path:
    path = dataset_path(dataset).joinpath(resource)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_dataset_resource(
    dataset: Dataset, resource: str, backfill: bool = True
) -> Path:
    path = dataset_resource_path(dataset, resource)
    if not path.exists() and backfill:
        bucket = get_backfill_bucket()
        if bucket is not None:
            blob_name = f"{BLOB_BASE}/{dataset.name}/{resource}"
            blob = bucket.get_blob(blob_name)
            if blob is not None:
                log.info(
                    "Backfilling dataset resource...",
                    dataset=dataset.name,
                    resource=resource,
                    blob_name=blob_name,
                )
                blob.download_to_filename(path)
    return path


def iter_dataset_entities(dataset: Dataset) -> Path:
    path = get_dataset_resource(dataset, FTM_RESOURCE)
    if path is None:
        raise ValueError(f"Cannot load entities for: {dataset.name}")
    for entity in path_entities(path, Entity):
        yield entity
