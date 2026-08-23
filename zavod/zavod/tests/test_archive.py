import pytest

from followthemoney.dataset import Version, VersionHistory

from zavod import settings
from zavod.meta import Dataset
from zavod.archive import archive_artifact, backfill_artifact, invalidate_dataset_urls
from zavod.archive import clear_data_path, dataset_data_path, dataset_resource_path
from zavod.archive import create_artifact_path, dataset_artifact_path
from zavod.archive import get_archive_backend, get_artifact_object
from zavod.archive import get_best_version, get_last_successful_version
from zavod.archive import publish_version_history
from zavod.archive import ARTIFACTS, DATASETS, LATEST, VERSIONS_FILE

RESOURCE_NAME = "foo.json"


def test_archive_artifact(testdataset1: Dataset):
    name = "foo.json"
    version = settings.RUN_VERSION
    data_path = dataset_data_path(testdataset1.name)
    local_path = dataset_resource_path(testdataset1.name, name)
    artifacts_root = settings.ARCHIVE_PATH / ARTIFACTS

    assert not local_path.exists()
    with open(local_path, "w") as fh:
        fh.write("hello, world!\n")

    # archive_artifact uploads to /artifacts/{ds}/{version}/.
    artifact_path = artifacts_root / testdataset1.name / version.id / name
    assert not artifact_path.exists()
    archive_artifact(local_path, testdataset1.name, version, name)
    assert artifact_path.exists()

    backend = get_archive_backend()
    assert backend.get_object(
        f"{ARTIFACTS}/{testdataset1.name}/{version.id}/{name}"
    ).exists()
    assert not backend.get_object(
        f"{ARTIFACTS}/{testdataset1.name}/{version.id}/{name}.xxx"
    ).exists()

    assert data_path.is_dir()
    clear_data_path(testdataset1.name)
    assert not data_path.exists()


def test_invalidate_dataset_urls(
    testdataset1: Dataset, monkeypatch: pytest.MonkeyPatch
):
    purged: list[str] = []
    monkeypatch.setattr("zavod.archive.invalidate_archive_cache", purged.append)

    invalidate_dataset_urls(testdataset1.name)
    assert purged == [
        f"{ARTIFACTS}/{testdataset1.name}/{VERSIONS_FILE}",
        f"{DATASETS}/{settings.RELEASE}/{testdataset1.name}/*",
        f"{DATASETS}/{LATEST}/{testdataset1.name}/*",
    ]


def _archive_run(
    dataset: Dataset,
    version: Version,
    successful: bool = True,
    archive_resource: bool = True,
) -> None:
    """Archive a run of the dataset: its copy of the resource, and the version
    history as it stands once the run is over - mirroring what publish_dataset
    does for a success and archive_failure for a failure."""
    create_artifact_path(dataset.name, version)
    if archive_resource:
        path = dataset_resource_path(dataset.name, RESOURCE_NAME)
        with open(path, "w") as fh:
            fh.write(version.id)
        archive_artifact(path, dataset.name, version, RESOURCE_NAME)
    if successful:
        vsn_path = dataset_artifact_path(dataset.name, version, VERSIONS_FILE)
        with open(vsn_path) as fh:
            history = VersionHistory.from_json(fh.read())
        history.last_successful = version
        with open(vsn_path, "w") as fh:
            fh.write(history.to_json())
    publish_version_history(dataset.name, version)


def test_version_selection_uses_last_successful_version(testdataset1: Dataset):
    """Version discovery answers with the last successful run, not the newest
    one. See https://github.com/opensanctions/operations/issues/2762"""
    succeeded = Version.from_string("20260101000000-aaa")
    failed = Version.from_string("20260102000000-bbb")
    _archive_run(testdataset1, succeeded)
    # The failed run even has a copy of the resource, so this can only pass by
    # consulting the version history, not by finding the newest copy:
    _archive_run(testdataset1, failed, successful=False)

    assert get_last_successful_version(testdataset1.name) == succeeded
    assert get_best_version(testdataset1.name) == succeeded

    prefix = f"{ARTIFACTS}/{testdataset1.name}"
    object = get_artifact_object(testdataset1.name, succeeded, RESOURCE_NAME)
    assert object is not None
    assert object.name == f"{prefix}/{succeeded.id}/{RESOURCE_NAME}"

    # A version the caller names explicitly is honoured, failed or not.
    object = get_artifact_object(testdataset1.name, failed, RESOURCE_NAME)
    assert object is not None
    assert object.name == f"{prefix}/{failed.id}/{RESOURCE_NAME}"


def test_version_selection_without_successful_version(testdataset1: Dataset):
    """Until a run has succeeded there is no last successful version; only
    get_best_version falls back to the newest run (for metadata purposes)."""
    failed = Version.from_string("20260101000000-aaa")
    _archive_run(testdataset1, failed, successful=False)

    assert get_last_successful_version(testdataset1.name) is None
    assert get_best_version(testdataset1.name) == failed


def test_get_artifact_object_does_not_mix_runs(testdataset1: Dataset):
    """A resource the last successful run didn't archive is not substituted from
    an earlier run, whose data the rest of the current metadata doesn't
    describe: artifact lookups pin an exact version."""
    older = Version.from_string("20260101000000-aaa")
    newer = Version.from_string("20260103000000-ccc")
    _archive_run(testdataset1, older)
    _archive_run(testdataset1, newer, archive_resource=False)

    assert get_last_successful_version(testdataset1.name) == newer
    assert get_artifact_object(testdataset1.name, newer, RESOURCE_NAME) is None


def test_artifact_backfill(testdataset1: Dataset):
    name = "foo.json"
    version = settings.RUN_VERSION
    local_path = dataset_resource_path(testdataset1.name, name)
    assert not local_path.exists()
    with open(local_path, "w") as fh:
        fh.write("hello, world!\n")

    artifacts_path = settings.ARCHIVE_PATH / ARTIFACTS / testdataset1.name
    archive_artifact(local_path, testdataset1.name, version, name)
    assert artifacts_path.is_dir()
    local_path.unlink()

    # A resource the version never archived cannot be backfilled:
    assert backfill_artifact(testdataset1.name, version, "missing.json") is None

    # Backfill of an explicitly named version works regardless of the version
    # history:
    path = backfill_artifact(testdataset1.name, version, name)
    assert path is not None
    assert path.read_text() == "hello, world!\n"

    # But nothing answers version discovery until the history is published:
    versions_file = artifacts_path / VERSIONS_FILE
    assert not versions_file.exists()
    assert get_best_version(testdataset1.name) is None

    _archive_run(testdataset1, version, archive_resource=False)
    assert versions_file.exists()
    best = get_best_version(testdataset1.name)
    assert best == version
    path = backfill_artifact(testdataset1.name, best, name)
    assert path is not None
    assert path.exists()
