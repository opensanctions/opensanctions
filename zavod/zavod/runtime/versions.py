from pathlib import Path
from typing import Optional
from nomenklatura.versions import Version, VersionHistory

from zavod.meta import Dataset
from zavod.archive import dataset_resource_path, get_versions_data
from zavod.archive import get_dataset_artifact
from zavod.archive import VERSIONS_FILE


def _versions_path(dataset_name: str) -> Path:
    return dataset_resource_path(dataset_name, VERSIONS_FILE)


def make_version(
    dataset: Dataset, version: Version, append_new_version_to_history: bool = False
) -> None:
    """Add a new version to the dataset history.

    Args:
        append_new_version_to_history: If True, a new version will be appended to the existing version history.
            If False, a new version will only be created if no versions exist yet.
    """
    path = _versions_path(dataset.name)
    if path.exists() and not append_new_version_to_history:
        return
    # get_versions_data always reads from the archive, never from the local file system.
    data = get_versions_data(dataset.name)
    history = VersionHistory.from_json(data or "{}")
    if version not in history.items:
        history = history.append(version)

    with open(path, "w") as fh:
        fh.write(history.to_json())


def set_last_successful_version(dataset: Dataset, version: Version) -> None:
    """Set the last successful version in the dataset history."""
    path = _versions_path(dataset.name)
    if not path.exists():
        raise RuntimeError(
            f"Version history file does not exist for dataset {dataset.name}"
        )
    with open(path, "r") as fh:
        history = VersionHistory.from_json(fh.read())
    if version not in history.items:
        raise RuntimeError(
            f"Version {version} is not in the version history for dataset {dataset.name}"
        )
    history.last_successful = version
    with open(path, "w") as fh:
        fh.write(history.to_json())


def get_history(dataset_name: str, backfill: bool = True) -> VersionHistory:
    """Get the version history for a dataset."""
    path = get_dataset_artifact(dataset_name, VERSIONS_FILE, backfill=backfill)
    if not path.exists():
        return VersionHistory([])
    with open(path, "r") as fh:
        return VersionHistory.from_json(fh.read())


def get_latest(dataset_name: str, backfill: bool = True) -> Optional[Version]:
    """Get the latest version for a dataset."""
    history = get_history(dataset_name, backfill=backfill)
    return history.latest
