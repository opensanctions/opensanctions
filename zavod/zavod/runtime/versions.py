from followthemoney.dataset import Version, VersionHistory

from zavod.meta import Dataset
from zavod.archive import dataset_artifact_path
from zavod.archive import VERSIONS_FILE


def set_version_successful(dataset: Dataset, version: str) -> None:
    """Set the last successful version in the dataset history."""
    path = dataset_artifact_path(dataset.name, version, VERSIONS_FILE)
    if not path.exists():
        raise RuntimeError(
            f"Version history file does not exist for dataset {dataset.name}"
        )
    with open(path) as fh:
        history = VersionHistory.from_json(fh.read())
    if version not in history.items:
        raise RuntimeError(
            f"Version {version} is not in the version history for dataset {dataset.name}"
        )
    history.last_successful = Version.from_string(version)
    with open(path, "w") as fh:
        fh.write(history.to_json())
