import pytest

from followthemoney.dataset import Version

from zavod import settings
from zavod.meta import Dataset
from zavod.runtime.versions import make_version, set_last_successful_version
from zavod.archive import get_dataset_artifact, invalidate_dataset_urls
from zavod.archive import archive_artifact
from zavod.archive import clear_data_path, dataset_data_path, dataset_resource_path
from zavod.archive import publish_version_history, get_archive_backend
from zavod.archive import get_artifact_object
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
    name = "foo.json"
    purged: list[str] = []
    monkeypatch.setattr("zavod.archive.invalidate_archive_cache", purged.append)

    invalidate_dataset_urls(testdataset1.name, name)
    assert purged == [
        f"{DATASETS}/{settings.RELEASE}/{testdataset1.name}/{name}",
        f"{DATASETS}/{LATEST}/{testdataset1.name}/{name}",
    ]


def _archive_run(
    dataset: Dataset,
    version: Version,
    successful: bool = True,
    archive_resource: bool = True,
) -> None:
    """Archive a run of the dataset: its copy of the resource, and the version
    history as it stands once the run is over."""
    if archive_resource:
        path = dataset_resource_path(dataset.name, RESOURCE_NAME)
        with open(path, "w") as fh:
            fh.write(version.id)
        archive_artifact(path, dataset.name, version, RESOURCE_NAME)
    make_version(dataset, version, append_new_version_to_history=True)
    if successful:
        set_last_successful_version(dataset, version)
    publish_version_history(dataset.name)


def test_get_artifact_object_uses_last_successful_version(testdataset1: Dataset):
    """A lookup which doesn't name a version answers with the last successful
    run, not the newest one. See https://github.com/opensanctions/operations/issues/2762"""
    succeeded = Version.from_string("20260101000000-aaa")
    failed = Version.from_string("20260102000000-bbb")
    _archive_run(testdataset1, succeeded)
    # The failed run even has a copy of the resource, so this can only pass by
    # consulting the version history, not by finding the newest copy:
    _archive_run(testdataset1, failed, successful=False)

    object = get_artifact_object(testdataset1.name, RESOURCE_NAME)
    assert object is not None
    prefix = f"{ARTIFACTS}/{testdataset1.name}"
    assert object.name == f"{prefix}/{succeeded.id}/{RESOURCE_NAME}"

    # A version the caller names explicitly is honoured, failed or not.
    # Maybe this doesn't have a use case as of 2026-08-11 but it's documenting behaviour.
    object = get_artifact_object(testdataset1.name, RESOURCE_NAME, version=failed.id)
    assert object is not None
    assert object.name == f"{prefix}/{failed.id}/{RESOURCE_NAME}"


def test_get_artifact_object_without_successful_version(testdataset1: Dataset):
    """Until a run has succeeded there is nothing to answer a lookup with."""
    _archive_run(testdataset1, Version.from_string("20260101000000-aaa"), False)

    assert get_artifact_object(testdataset1.name, RESOURCE_NAME) is None


def test_get_artifact_object_does_not_mix_runs(testdataset1: Dataset):
    """A resource the last successful run didn't archive is not substituted from
    an earlier run, whose data the rest of the current metadata doesn't
    describe."""
    older = Version.from_string("20260101000000-aaa")
    newer = Version.from_string("20260103000000-ccc")
    _archive_run(testdataset1, older)
    _archive_run(testdataset1, newer, archive_resource=False)

    assert get_artifact_object(testdataset1.name, RESOURCE_NAME) is None


def test_artifact_backfill(testdataset1: Dataset):
    name = "foo.json"
    local_path = dataset_resource_path(testdataset1.name, name)
    assert not local_path.exists()
    with open(local_path, "w") as fh:
        fh.write("hello, world!\n")

    artifacts_path = settings.ARCHIVE_PATH / ARTIFACTS / testdataset1.name
    archive_artifact(local_path, testdataset1.name, settings.RUN_VERSION, name)
    assert artifacts_path.is_dir()
    local_path.unlink()
    local_path = get_dataset_artifact(testdataset1.name, name)
    # Data is unpublished:
    versions_file = artifacts_path / VERSIONS_FILE
    assert not versions_file.exists()
    assert not local_path.exists()
    make_version(testdataset1, settings.RUN_VERSION)
    # Backfill answers with the last successful run,
    # so the run has to be recorded as one:
    set_last_successful_version(testdataset1, settings.RUN_VERSION)
    publish_version_history(testdataset1.name)
    assert versions_file.exists()
    local_path = get_dataset_artifact(testdataset1.name, name)
    assert local_path.exists()
