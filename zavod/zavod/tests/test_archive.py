import shutil
from zavod import settings
from zavod.meta import Dataset
from zavod.archive import get_dataset_resource, publish_resource
from zavod.archive import clear_data_path, dataset_data_path
from zavod.archive import DATASETS


def test_archive_publish(testdataset1: Dataset):
    name = "foo.json"
    data_path = dataset_data_path(testdataset1.name)
    local_path = get_dataset_resource(testdataset1, name)
    dataset_archive_path = settings.ARCHIVE_PATH / DATASETS
    assert not local_path.exists()
    with open(local_path, "w") as fh:
        fh.write("hello, world!\n")
    archive_path = dataset_archive_path / settings.RELEASE / testdataset1.name / name
    assert not archive_path.exists()
    publish_resource(local_path, testdataset1.name, name, latest=False)
    assert archive_path.exists()
    local_path.unlink()
    local_path = get_dataset_resource(testdataset1, name)
    assert not local_path.exists()

    with open(local_path, "w") as fh:
        fh.write("hello, world!\n")

    latest_path = dataset_archive_path / "latest" / testdataset1.name / name
    assert not latest_path.exists()
    publish_resource(local_path, testdataset1.name, name, latest=True)
    assert latest_path.exists()

    local_path = get_dataset_resource(testdataset1, name)
    assert local_path.exists()
    shutil.rmtree(dataset_archive_path / "latest")
    assert data_path.is_dir()
    clear_data_path(testdataset1.name)
    assert not data_path.exists()
