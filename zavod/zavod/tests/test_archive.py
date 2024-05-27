import shutil
from zavod import settings
from zavod.meta import Dataset
from zavod.archive import get_dataset_artifact, publish_resource, publish_artifact
from zavod.archive import clear_data_path, dataset_data_path, dataset_resource_path
from zavod.archive import publish_dataset_history
from zavod.archive import DATASETS, ARTIFACTS, VERSIONS_FILE


def test_archive_publish(testdataset1: Dataset):
    name = "foo.json"
    data_path = dataset_data_path(testdataset1.name)
    local_path = dataset_resource_path(testdataset1.name, name)
    dataset_archive_path = settings.ARCHIVE_PATH / DATASETS
    assert not local_path.exists()
    with open(local_path, "w") as fh:
        fh.write("hello, world!\n")
    archive_path = dataset_archive_path / settings.RELEASE / testdataset1.name / name
    assert not archive_path.exists()
    publish_resource(local_path, testdataset1.name, name, latest=False)
    assert archive_path.exists()

    latest_path = dataset_archive_path / "latest" / testdataset1.name / name
    assert not latest_path.exists()
    publish_resource(local_path, testdataset1.name, name, latest=True)
    assert latest_path.exists()

    shutil.rmtree(dataset_archive_path / "latest")
    assert data_path.is_dir()
    clear_data_path(testdataset1.name)
    assert not data_path.exists()


def test_artifact_backfill(testdataset1: Dataset):
    name = "foo.json"
    local_path = dataset_resource_path(testdataset1.name, name)
    assert not local_path.exists()
    with open(local_path, "w") as fh:
        fh.write("hello, world!\n")

    artifacts_path = settings.ARCHIVE_PATH / ARTIFACTS / testdataset1.name
    publish_artifact(local_path, testdataset1.name, settings.RUN_VERSION, name)
    assert artifacts_path.is_dir()
    local_path.unlink()
    local_path = get_dataset_artifact(testdataset1.name, name)
    # Data is unpublished:
    versions_file = artifacts_path / VERSIONS_FILE
    assert not versions_file.exists()
    assert not local_path.exists()
    publish_dataset_history(testdataset1.name, settings.RUN_VERSION)
    assert versions_file.exists()
    local_path = get_dataset_artifact(testdataset1.name, name)
    assert local_path.exists()
