import shutil
from zavod import settings
from zavod.meta import Dataset
from zavod.archive import STATISTICS_FILE, INDEX_FILE, STATEMENTS_FILE
from zavod.archive import dataset_path, get_dataset_resource
from zavod.archive import iter_dataset_statements, iter_previous_statements
from zavod.runner import run_dataset
from zavod.store import get_view, clear_store
from zavod.exporters import export_dataset
from zavod.publish import publish_dataset, publish_failure
from zavod.exc import RunFailedException


def test_publish_dataset(testdataset1: Dataset):
    release_path = settings.ARCHIVE_PATH / settings.RELEASE / testdataset1.name
    latest_path = settings.ARCHIVE_PATH / "latest" / testdataset1.name
    assert not release_path.joinpath(INDEX_FILE).exists()
    assert not latest_path.joinpath(INDEX_FILE).exists()
    clear_store(testdataset1)
    run_dataset(testdataset1)
    view = get_view(testdataset1)
    export_dataset(testdataset1, view)

    publish_dataset(testdataset1, latest=False)

    assert release_path.joinpath(INDEX_FILE).exists()
    assert not latest_path.joinpath(INDEX_FILE).exists()
    assert release_path.joinpath(STATEMENTS_FILE).exists()
    assert release_path.joinpath(STATISTICS_FILE).exists()

    publish_dataset(testdataset1, latest=True)
    assert latest_path.joinpath(INDEX_FILE).exists()

    # Test backfill:
    shutil.rmtree(dataset_path(testdataset1.name))
    assert len(list(iter_dataset_statements(testdataset1))) > 5
    assert len(list(iter_previous_statements(testdataset1))) > 5
    path = get_dataset_resource(testdataset1, INDEX_FILE, backfill=False)
    assert not path.exists()
    path = get_dataset_resource(testdataset1, INDEX_FILE, backfill=True)
    assert path.exists()

    shutil.rmtree(latest_path)
    shutil.rmtree(release_path)


def test_publish_failure(testdataset1: Dataset):
    latest_path = settings.ARCHIVE_PATH / "latest" / testdataset1.name
    testdataset1.data.format = "FAIL"
    try:
        run_dataset(testdataset1)
    except RunFailedException:
        publish_failure(testdataset1, latest=True)
    shutil.rmtree(dataset_path(testdataset1.name))

    assert not latest_path.joinpath("statements.pack").exists()
    assert latest_path.joinpath("index.json").exists()
    assert latest_path.joinpath("issues.json").exists()
    shutil.rmtree(latest_path)
