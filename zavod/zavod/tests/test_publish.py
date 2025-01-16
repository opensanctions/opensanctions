from typing import Optional
from nomenklatura.versions import VersionHistory

from zavod import settings
from zavod.meta import Dataset
from zavod.archive import get_dataset_artifact, clear_data_path
from zavod.archive import iter_dataset_statements, iter_previous_statements
from zavod.archive import STATISTICS_FILE, INDEX_FILE, STATEMENTS_FILE
from zavod.archive import DATASETS, ARTIFACTS, VERSIONS_FILE
from zavod.crawl import crawl_dataset
from zavod.store import get_store
from zavod.exporters import export_dataset
from zavod.integration import get_dataset_linker
from zavod.publish import publish_dataset, publish_failure
from zavod.exc import RunFailedException


def _read_history(dataset_name: str) -> Optional[VersionHistory]:
    fn = settings.ARCHIVE_PATH / ARTIFACTS / dataset_name / VERSIONS_FILE
    if not fn.exists():
        return None
    with open(fn, "r") as fh:
        return VersionHistory.from_json(fh.read())


def test_publish_dataset(testdataset1: Dataset):
    linker = get_dataset_linker(testdataset1)
    art_path = settings.ARCHIVE_PATH / ARTIFACTS
    arch_path = settings.ARCHIVE_PATH / DATASETS
    release_path = arch_path / settings.RELEASE / testdataset1.name
    latest_path = arch_path / "latest" / testdataset1.name
    assert not release_path.joinpath(INDEX_FILE).exists()
    assert not latest_path.joinpath(INDEX_FILE).exists()
    history = _read_history(testdataset1.name)
    assert history is None
    crawl_dataset(testdataset1)
    store = get_store(testdataset1, linker)
    store.sync()
    view = store.view(testdataset1)
    export_dataset(testdataset1, view)

    publish_dataset(testdataset1, latest=False)
    history = _read_history(testdataset1.name)
    assert history is not None
    assert history.latest is not None
    assert history.latest.id is not None
    artifact_path = art_path / testdataset1.name / history.latest.id
    assert artifact_path.exists()
    assert artifact_path.joinpath(STATEMENTS_FILE).exists()
    assert artifact_path.joinpath(STATEMENTS_FILE).exists()
    assert artifact_path.joinpath(STATISTICS_FILE).exists()

    assert release_path.joinpath(INDEX_FILE).exists()
    assert not release_path.joinpath(STATEMENTS_FILE).exists()
    assert not release_path.joinpath(STATISTICS_FILE).exists()

    assert not latest_path.joinpath(INDEX_FILE).exists()
    assert release_path.joinpath("entities.ftm.json").exists()

    publish_dataset(testdataset1, latest=True)
    assert latest_path.joinpath(INDEX_FILE).exists()

    # Test backfill:
    clear_data_path(testdataset1.name)
    assert len(list(iter_dataset_statements(testdataset1))) > 5
    assert len(list(iter_previous_statements(testdataset1))) > 5
    path = get_dataset_artifact(testdataset1.name, INDEX_FILE, backfill=False)
    assert not path.exists()
    path = get_dataset_artifact(testdataset1.name, INDEX_FILE, backfill=True)
    assert path.exists()


def test_publish_failure(testdataset1: Dataset):
    arch_path = settings.ARCHIVE_PATH / DATASETS
    art_path = settings.ARCHIVE_PATH / ARTIFACTS
    latest_path = arch_path / "latest" / testdataset1.name
    assert testdataset1.data is not None
    testdataset1.data.format = "FAIL"
    try:
        crawl_dataset(testdataset1)
    except RunFailedException:
        publish_failure(testdataset1, latest=True)
    clear_data_path(testdataset1.name)

    history = _read_history(testdataset1.name)
    assert history is not None
    assert history.latest is not None
    assert history.latest.id is not None
    artifact_path = art_path / testdataset1.name / history.latest.id

    assert not latest_path.joinpath("statements.pack").exists()
    assert not latest_path.joinpath("issues.json").exists()
    assert artifact_path.joinpath("issues.json").exists()
    assert artifact_path.joinpath("index.json").exists()
    assert latest_path.joinpath("index.json").exists()
