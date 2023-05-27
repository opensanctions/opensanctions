from pathlib import Path
from functools import cache
from typing import Optional, Generator
from zavod.logs import get_logger
from google.cloud.storage import Client, Bucket, Blob
from followthemoney.cli.util import path_entities
from nomenklatura.statement import Statement, JSON
from nomenklatura.statement.serialize import read_path_statements, read_statements

from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.collection import Collection
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
    dataset: Dataset, resource: str, backfill: bool = True
) -> Optional[Path]:
    path = dataset_resource_path(dataset, resource)
    if path.exists():
        return path
    if backfill:
        return backfill_resource(dataset, resource, path)
    return None


def iter_dataset_entities(dataset: Dataset) -> Generator[Entity, None, None]:
    path = get_dataset_resource(dataset, FTM_RESOURCE)
    if path is None:
        raise ValueError(f"Cannot load entities for: {dataset.name}")
    for entity in path_entities(path, Entity):
        yield entity


def iter_dataset_statements(dataset: Dataset) -> Generator[Statement, None, None]:
    if dataset.TYPE == Collection.TYPE:
        for source in dataset.children:
            yield from iter_previous_statements(source)
        return

    path = get_dataset_resource(dataset, STATEMENTS_RESOURCE)
    if path is None:
        raise ValueError(f"Cannot load statements for: {dataset.name}")
    for stmt in read_path_statements(path, JSON, Statement):
        yield stmt


def iter_previous_statements(dataset: Dataset) -> Generator[Statement, None, None]:
    # TODO: loading this from database for now

    # resource = "statements.previous.json"
    # path = dataset_resource_path(dataset, resource)
    # if not path.exists():
    #     path = backfill_resource(dataset, STATEMENTS_RESOURCE, path)
    backfill_blob = get_backfill_blob(dataset, STATEMENTS_RESOURCE)
    if backfill_blob is not None:
        with backfill_blob.open("rb") as fh:
            yield from read_statements(fh, JSON, Statement)
        return

    # current_path = dataset_resource_path(dataset, STATEMENTS_RESOURCE)
    # if current_path.exists():
    #     path = current_path
    # if path is None:
    #     log.warning(f"Cannot load previous statements for: {dataset.name}")
    #     return
    # for stmt in read_path_statements(path, JSON, Statement):
    #     yield stmt
