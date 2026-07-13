from followthemoney.dataset import Version, VersionHistory

from zavod.meta import Dataset
from zavod.archive import dataset_resource_path, get_versions_data
from zavod.archive import VERSIONS_FILE


def make_version(dataset: Dataset, version: Version) -> None:
    """Record a new run version of the dataset by writing the version history file
    (the history known to the archive, plus this version) into the local working
    directory of that version.

    Only the start of a new run (a crawl, or the start of a collection run) should
    do this - all other stages operate on a version that already exists."""
    path = dataset_resource_path(dataset.name, version, VERSIONS_FILE)
    if path.exists():
        return
    # get_versions_data always reads from the archive, never from the local file system.
    data = get_versions_data(dataset.name)
    history = VersionHistory.from_json(data or "{}")
    if version not in history.items:
        history = history.append(version)

    with open(path, "w") as fh:
        fh.write(history.to_json())


def set_last_successful_version(dataset: Dataset, version: Version) -> None:
    """Set the last successful version in the given run's version history file."""
    path = dataset_resource_path(dataset.name, version, VERSIONS_FILE)
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
