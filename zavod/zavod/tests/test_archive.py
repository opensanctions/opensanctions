import shutil

from followthemoney.dataset import Version

from zavod import settings
from zavod.meta import Dataset
from zavod.runtime.versions import make_version
from zavod.archive import get_dataset_artifact, publish_artifact, archive_artifact
from zavod.archive import clear_data_path, dataset_data_path, dataset_resource_path
from zavod.archive import publish_version_history, get_archive_backend
from zavod.archive import ARTIFACTS, DATASETS, LATEST, VERSIONS_FILE


def test_archive_then_publish(testdataset1: Dataset):
    name = "foo.json"
    version = settings.RUN_VERSION
    data_path = dataset_data_path(testdataset1.name)
    local_path = dataset_resource_path(testdataset1.name, version, name)
    artifacts_root = settings.ARCHIVE_PATH / ARTIFACTS
    datasets_root = settings.ARCHIVE_PATH / DATASETS

    assert not local_path.exists()
    with open(local_path, "w") as fh:
        fh.write("hello, world!\n")

    # archive_artifact uploads to /artifacts/{ds}/{version}/.
    artifact_path = artifacts_root / testdataset1.name / version.id / name
    assert not artifact_path.exists()
    archive_artifact(local_path, testdataset1.name, version, name)
    assert artifact_path.exists()

    # publish_artifact then server-side copies into /datasets/{RELEASE}/.
    release_path = datasets_root / settings.RELEASE / testdataset1.name / name
    assert not release_path.exists()
    publish_artifact(testdataset1.name, version.id, name, republish_to_latest=False)
    assert release_path.exists()
    assert release_path.read_bytes() == artifact_path.read_bytes()

    # republish_to_latest=True also writes /datasets/latest/.
    latest_path = datasets_root / LATEST / testdataset1.name / name
    assert not latest_path.exists()
    publish_artifact(testdataset1.name, version.id, name, republish_to_latest=True)
    assert latest_path.exists()
    assert latest_path.read_bytes() == artifact_path.read_bytes()

    backend = get_archive_backend()
    assert backend.get_object(
        f"{DATASETS}/{settings.RELEASE}/{testdataset1.name}/{name}"
    ).exists()
    assert not backend.get_object(
        f"{DATASETS}/{settings.RELEASE}/{testdataset1.name}/{name}.xxx"
    ).exists()

    shutil.rmtree(datasets_root / LATEST)
    assert data_path.is_dir()
    clear_data_path(testdataset1.name)
    assert not data_path.exists()


def test_artifact_backfill(testdataset1: Dataset):
    name = "foo.json"
    version = settings.RUN_VERSION
    local_path = dataset_resource_path(testdataset1.name, version, name)
    assert not local_path.exists()
    with open(local_path, "w") as fh:
        fh.write("hello, world!\n")

    artifacts_path = settings.ARCHIVE_PATH / ARTIFACTS / testdataset1.name
    archive_artifact(local_path, testdataset1.name, version, name)
    assert artifacts_path.is_dir()
    local_path.unlink()
    assert not local_path.exists()

    # Backfilling references an exact version and does not depend on a published
    # version history existing.
    versions_file = artifacts_path / VERSIONS_FILE
    assert not versions_file.exists()
    backfilled = get_dataset_artifact(testdataset1.name, version, name)
    assert backfilled.exists()
    assert backfilled.read_text() == "hello, world!\n"

    # A different version has no such artifact, so nothing is backfilled.
    backfilled.unlink()
    other_version = Version.new("zzz")
    missing = get_dataset_artifact(testdataset1.name, other_version, name)
    assert not missing.exists()

    # Publishing the version history writes the dataset's root versions.json.
    make_version(testdataset1, version)
    publish_version_history(testdataset1.name, version)
    assert versions_file.exists()
