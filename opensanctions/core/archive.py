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


# def iter_dataset_entities(dataset: Dataset) -> Generator[Entity, None, None]:
#     path = get_dataset_resource(dataset, FTM_RESOURCE)
#     if path is None:
#         raise ValueError(f"Cannot load entities for: {dataset.name}")
#     for entity in path_entities(path, Entity):
#         yield entity


def read_fh_statements(fh: BinaryIO, external: bool) -> StatementGen:
    while line := fh.readline(MAX_LINE):
        data = orjson.loads(line)
        if not external and data["external"]:
            continue
        yield Statement.from_dict(data)


def iter_dataset_statements(dataset: Dataset, external: bool = True) -> StatementGen:
    for scope in dataset.scopes:
        yield from _iter_scope_statements(scope, external=external)


def _iter_scope_statements(dataset: Dataset, external: bool = True) -> StatementGen:
    path = dataset_resource_path(dataset, STATEMENTS_RESOURCE)
    if not path.exists():
        backfill_blob = get_backfill_blob(dataset, STATEMENTS_RESOURCE)
        if backfill_blob is not None:
            log.info(
                "Streaming backfilled statements...",
                dataset=dataset.name,
            )
            with backfill_blob.open("rb") as fh:
                yield from read_fh_statements(fh, external)
            return
        raise ValueError(f"Cannot load statements for: {dataset.name}")

    with open(path, "rb") as fh:
        yield from read_fh_statements(fh, external)


def explicit_backfill(dataset: Dataset, force: bool = False) -> None:
    get_dataset_resource(dataset, INDEX_RESOURCE, force_backfill=force)

    for scope in dataset.scopes:
        if scope.name != dataset.name:
            explicit_backfill(scope, force=force)

    if dataset.TYPE == Collection.TYPE:
        return

    # log.info("Fetch statements", dataset=dataset.name)
    get_dataset_resource(dataset, STATEMENTS_RESOURCE, force_backfill=force)


def iter_previous_statements(dataset: Dataset, external: bool = False) -> StatementGen:
    # TODO: loading this from database for now

    resource = "statements.previous.json"
    path = dataset_resource_path(dataset, resource)
    if not path.exists():
        #     path = backfill_resource(dataset, STATEMENTS_RESOURCE, path)
        backfill_blob = get_backfill_blob(dataset, STATEMENTS_RESOURCE)
        if backfill_blob is not None:
            with backfill_blob.open("rb") as fh:
                yield from read_fh_statements(fh, external=external)
            return

        current_path = dataset_resource_path(dataset, STATEMENTS_RESOURCE)
        if current_path.exists():
            shutil.copy(current_path, path)

    if not path.exists():
        log.warning(f"Cannot load previous statements for: {dataset.name}")
        return

    with open(path, "rb") as fh:
        yield from read_fh_statements(fh, external=external)
