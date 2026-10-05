from enum import StrEnum
import json
from typing import Any

from pydantic import ValidationError

from followthemoney.dataset import Version

from rigour.time import datetime_iso

from zavod import settings
from zavod.logs import get_logger
from zavod.meta import Dataset, get_catalog
from zavod.exporters.metadata.model import CatalogDatasetModel
from zavod.archive import (
    INDEX_FILE,
    STATISTICS_FILE,
    ISSUES_FILE,
    backfill_artifact,
    dataset_artifact_path,
    get_last_successful_version,
)
from zavod.archive import CATALOG_FILE, DELTA_INDEX_FILE, DELTA_EXPORT_FILE
from zavod.archive import UNLISTED_RESOURCES
from zavod.archive import get_artifact_object, iter_dataset_versions
from zavod.runtime.urls import make_artifact_url
from zavod.runtime.resources import DatasetResources
from zavod.runtime.issues import DatasetIssues
from zavod.util import write_json

log = get_logger(__name__)


class DatasetVersionResult(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"


def get_base_dataset_metadata(
    dataset: Dataset, version: Version, result: DatasetVersionResult
) -> dict[str, Any]:
    """Build the barebones metadata block for a dataset, without artifact URLs."""
    version_time_iso = datetime_iso(version.dt)

    meta: dict[str, Any] = {
        "issue_levels": {},
        "issue_count": 0,
        "updated_at": version_time_iso,
        "resources": [],
    }
    if result != DatasetVersionResult.SUCCESS:
        return meta

    # Entity counts come from the statistics exporter's output for this run
    # only: a failed run publishes no statistics, so its index carries no counts.
    statistics_path = dataset_artifact_path(dataset.name, version, STATISTICS_FILE)
    if statistics_path.is_file():
        with open(statistics_path) as fh:
            stats: dict[str, Any] = json.load(fh)
            meta["entity_count"] = stats.get("entity_count", 0)
            targets = stats.get("targets", {})
            meta["target_count"] = targets.get("total", 0)
            things = stats.get("things", {})
            meta["thing_count"] = things.get("total", 0)

            ## last_change limitations:
            #
            ### Always updates when dataset is empty
            # While Statistics calculates last_change as max(last_change) of entities,
            # an empty export has no entity to derive last_change from, so we fall back
            # to the crawl run time, meaning it always updates even without changes.
            #
            ### Can go back in time when the latest changed entity gets removed
            # Because removing max(last_change) means it's now a less recently updated
            # entity's last_change.
            #
            ### Doesn't change when only change is removal of any other entity
            # Because last_change is derived from the max(last_change) of entities,
            # removing an entity that is not the most recently changed one does not affect it.

            last_change = stats.get("last_change")
            if last_change is None:
                last_change = version_time_iso
            meta["last_change"] = last_change

    res_datas: list[dict[str, Any]] = []
    for res in DatasetResources(dataset, version).all():
        if res.name in UNLISTED_RESOURCES:
            continue
        res_data = res.model_dump(mode="json", exclude_none=True)
        res_data["path"] = res.name
        res_datas.append(res_data)
    meta["resources"] = res_datas
    return meta


def write_dataset_index(
    dataset: Dataset, version: Version, result: DatasetVersionResult
) -> None:
    """Export dataset metadata to index.json."""
    catalog = get_catalog()
    index_path = dataset_artifact_path(dataset.name, version, INDEX_FILE)
    log.info(
        "Writing dataset index",
        path=index_path,
        version=version.id,
        is_collection=dataset.is_collection,
    )
    meta = get_base_dataset_metadata(dataset, version, result)
    meta.update(dataset.to_opensanctions_dict(catalog))

    # Remove redundant dataset hierarchy metadata
    # (see https://www.opensanctions.org/changelog/2/):
    meta.pop("externals", None)
    meta.pop("sources", None)
    meta.pop("collections", None)

    meta["version"] = version.id
    meta["updated_at"] = datetime_iso(version.dt)
    meta["index_url"] = make_artifact_url(dataset.name, version, INDEX_FILE)
    for res_data in meta["resources"]:
        res_data["url"] = make_artifact_url(dataset.name, version, res_data["path"])

    issues = DatasetIssues(dataset, version)
    meta["issue_levels"] = issues.by_level()
    meta["issue_count"] = sum(meta["issue_levels"].values())
    meta["last_export"] = settings.RUN_TIME_ISO
    meta["result"] = result.value
    # NOTE: when adding a another URL here, make sure to update Delivery Service,
    # it has a static list of URLs to rewrite
    meta["issues_url"] = make_artifact_url(dataset.name, version, ISSUES_FILE)
    meta["statistics_url"] = make_artifact_url(dataset.name, version, STATISTICS_FILE)

    delta_index_path = dataset_artifact_path(dataset.name, version, DELTA_INDEX_FILE)
    if result == DatasetVersionResult.SUCCESS and delta_index_path.is_file():
        # Only generated and published for successful exports:
        meta["delta_url"] = make_artifact_url(dataset.name, version, DELTA_INDEX_FILE)
    else:
        # If the delta index is not available, try to find the newest delta index
        # generate the URL from that:
        for prev_version in iter_dataset_versions(dataset.name):
            object = get_artifact_object(dataset.name, prev_version, DELTA_EXPORT_FILE)
            if object is not None:
                meta["delta_url"] = make_artifact_url(
                    dataset.name, prev_version, DELTA_INDEX_FILE
                )
                break

    # Validate against the published output contract before writing. The model
    # knows a failed run legitimately lacks statistics, so a degraded failure
    # index doesn't trip it.
    # TODO: For now this only warns so we can gauge how often real runs violate
    # the contract; tighten it to a hard failure once we're confident
    # (https://github.com/opensanctions/opensanctions/issues/4643).
    try:
        CatalogDatasetModel.model_validate(meta)
    except ValidationError as exc:
        log.warning(
            "Dataset metadata does not conform to the catalog model",
            dataset=dataset.name,
            errors=exc.errors(),
        )

    with open(index_path, "wb") as fh:
        write_json(meta, fh)


def get_catalog_dataset(
    dataset: Dataset, version: Version | None = None
) -> dict[str, Any] | None:
    """Build one catalog entry for a dataset.

    Args:
        dataset: The dataset to describe.
        version: The version being produced by the current run, when the dataset
            is the one being exported. ``None`` selects the dataset's last
            successful version in the archive.

    The function reads operational metadata from the version's index artifact. It
    then patches in the current metadata from the local dataset definition. This
    allows metadata corrections to appear without another dataset export.

    The function emits a warning and returns ``None`` when no version is given and
    the dataset has no last successful version.

    Returns:
        The combined catalog metadata, or ``None`` when no version is available.

    Raises:
        RuntimeError: The selected version has no index artifact.
    """
    if version is None:
        version = get_last_successful_version(dataset.name)
    if version is None:
        # Only datasets that have never completed a successful run reach this
        # edge case, so no run metadata exists for their catalog entries.
        log.warning(
            f"No last successful version found for {dataset.name}, "
            "returning None from get_catalog_dataset",
            dataset=dataset.name,
        )
        return None

    index_file_path = backfill_artifact(dataset.name, version, INDEX_FILE)
    if index_file_path is None:
        raise RuntimeError(
            f"No index file found for {dataset.name} at version {version.id}"
        )
    with open(index_file_path) as fh:
        meta: dict[str, Any] = json.load(fh)

    # Overwrite with latest metadata (without any run information), useful to quickly patch up the catalog
    # for datasets that don't get exported often.
    meta.update(dataset.to_opensanctions_dict(get_catalog()))
    return meta


def get_catalog_datasets(
    scope: Dataset, version: Version | None = None
) -> list[dict[str, Any]]:
    """Build catalog entries for every dataset in a scope.

    Args:
        scope: The collection to enumerate.
        version: The version being produced by the current run of the scope. When
            given, the scope's own entry describes this run instead of its last
            successful version in the archive. ``None`` when the scope is not being
            produced, as in the root catalog refresh.

    The result includes the scope itself, all nested collection scopes, and all leaf
    datasets. Every entry except the scope's own uses its last successful version.

    Each entry reads operational metadata from the selected version's index artifact.
    It then patches in the current metadata from the local dataset definition. This
    allows metadata corrections to appear without another dataset export.

    Zavod writes these entries to the dataset-level ``catalog.json`` for each
    collection export. Kombinat also uses them to write the frequently refreshed
    root catalog at ``datasets/latest/index.json``.

    The function omits a dataset without a last successful version, but this is a
    real edge case that only occurs when a dataset has never completed a successful
    run.

    This is a bit of a legacy function because its entries can reference a somewhat
    willy-nilly mix of versions instead of versions pinned by a Manifest. Yente
    relies on these catalogs and requires them to include nested collection scopes,
    so the catalog format is unlikely to change.

    Returns:
        Metadata dictionaries for the scope, its nested collections, and its leaves.
    """
    datasets = []
    for dataset in scope.datasets:
        # The scope's own run is not yet in the archive's version history while
        # its catalog is written, so its entry must come from the run version.
        dataset_version = version if dataset.name == scope.name else None
        catalog_dataset = get_catalog_dataset(dataset, dataset_version)
        # This is a real edge case. It only occurs for new datasets that have never
        # completed a successful run. They have no export to advertise. A later
        # successful run adds them to the catalog.
        if catalog_dataset is not None:
            datasets.append(catalog_dataset)
    return datasets


def write_delta_index(
    dataset: Dataset, version: Version, max_versions: int = 100
) -> None:
    """Export list of delta data versions for the dataset with their URLs
    associated."""
    versions: dict[str, str] = {}

    # This hasn't been uploaded yet, but will become available at the same
    # time as the index file:
    data_path = dataset_artifact_path(dataset.name, version, DELTA_EXPORT_FILE)
    if data_path.is_file() and data_path.stat().st_size > 0:
        versions[version.id] = make_artifact_url(
            dataset.name, version, DELTA_EXPORT_FILE
        )

    # Get the most recent versions of the dataset:
    for prev_version in iter_dataset_versions(dataset.name):
        if prev_version.id in versions:
            continue
        object = get_artifact_object(dataset.name, prev_version, DELTA_EXPORT_FILE)
        if object is not None and object.size() > 0:
            versions[prev_version.id] = make_artifact_url(
                dataset.name, prev_version, DELTA_EXPORT_FILE
            )
        if len(versions) >= max_versions:
            break

    # Alternatively as a list for tooling that doesn't support iterating
    # over object keys https://github.com/opensanctions/opensanctions/issues/2216
    # Unstable key because we anticipate replacing this with functionality in
    # the upcoming data delivery service, so we don't want to suggest that this
    # is generally available.
    version_list = [
        {
            "version": version_id,
            "url": version_url,
        }
        for version_id, version_url in versions.items()
    ]

    if len(versions) == 0:
        log.info(f"No delta versions found: {dataset.name}")
        return
    index_path = dataset_artifact_path(dataset.name, version, DELTA_INDEX_FILE)
    log.info("Writing delta versions index...", path=index_path.as_posix())
    with open(index_path, "wb") as fh:
        data = {
            "versions": versions,
            "unstable": {"version_list": version_list},
        }
        write_json(data, fh)


def write_catalog(scope: Dataset, version: Version) -> None:
    """Write the dataset-level ``catalog.json`` for a collection scope.

    The file contains catalog entries for the collection itself, its nested
    collections, and its leaf datasets. The collection's own entry describes the
    current run. Every other entry uses its independently selected last successful
    version. The function does nothing for a leaf dataset.

    Returns:
        None.
    """
    if not scope.is_collection:
        return
    catalog_path = dataset_artifact_path(scope.name, version, CATALOG_FILE)
    log.info("Writing collection as catalog...", path=catalog_path.as_posix())
    with open(catalog_path, "wb") as fh:
        data = {
            "datasets": get_catalog_datasets(scope, version),
            "updated_at": settings.RUN_TIME_ISO,
        }
        write_json(data, fh)
