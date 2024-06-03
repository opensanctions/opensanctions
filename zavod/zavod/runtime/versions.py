from pathlib import Path
from typing import Optional
from nomenklatura.versions import Version, VersionHistory

from zavod import settings
from zavod.archive import dataset_resource_path, get_versions_data
from zavod.archive import get_dataset_artifact
from zavod.archive import VERSIONS_FILE


def _versions_path(dataset_name: str) -> Path:
    return dataset_resource_path(dataset_name, VERSIONS_FILE)


def make_version(
    dataset_name: str, version: Version = settings.RUN_VERSION, overwrite: bool = False
) -> None:
    """Add a new version to the dataset history."""
    path = _versions_path(dataset_name)
    if path.exists() and not overwrite:
        return
    data = get_versions_data(dataset_name)
    history = VersionHistory.from_json(data or "{}")
    if version not in history.items:
        history = history.append(version)

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
