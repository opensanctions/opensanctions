from datetime import timedelta
from shutil import copyfile
from tempfile import mkdtemp
from pathlib import Path

from zavod import settings
from zavod.meta import Dataset
from zavod.runner import run_dataset
from zavod.store import get_store, get_view, clear_store
from zavod.tests.conftest import FIXTURES_PATH


def test_store_access(vdataset: Dataset):
    run_dataset(vdataset)
    store = get_store(vdataset, external=True)
    view = store.default_view(external=True)
    assert len(list(view.entities())) > 5, list(view.entities())
    entity = view.get_entity("osv-john-doe")
    assert entity is not None, entity
    assert entity.id == "osv-john-doe"
    store.close()

    view2 = get_view(vdataset, external=True)
    entity = view2.get_entity("osv-john-doe")
    assert entity is not None, entity
    assert entity.id == "osv-john-doe"
    assert entity.schema.name == "Person"
    assert entity.target is True
    # assert entity.external is False
    assert entity.last_change == settings.RUN_TIME_ISO
    assert entity.first_seen == settings.RUN_TIME_ISO
    assert entity.last_seen == settings.RUN_TIME_ISO
    view2.store.close()
    clear_store(vdataset)
    assert not store.path.exists()


def test_timestamps(vdataset: Dataset):
    settings.ARCHIVE_BACKEND = "FilesystemBackend"
    settings.ARCHIVE_PATH = Path(mkdtemp())
    first_time = settings.RUN_TIME_ISO
    run_dataset(vdataset)

    archive_path = settings.ARCHIVE_PATH / "datasets/latest/testdataset1"
    archive_path.mkdir(parents=True, exist_ok=True)
    copyfile(
        settings.DATA_PATH / "datasets" / vdataset.name / "statements.pack",
        archive_path / "statements.pack",
    )

    settings.RUN_TIME = settings.RUN_TIME + timedelta(days=1)
    settings.RUN_TIME_ISO = settings.RUN_TIME.isoformat(sep="T", timespec="seconds")
    settings.RUN_DATE = settings.RUN_TIME.date().isoformat()
    second_time = settings.RUN_TIME_ISO
    run_dataset(vdataset)

    store = get_store(vdataset, external=True)
    view = store.default_view(external=True)
    entity = view.get_entity("osv-john-doe")

    settings.ARCHIVE_BACKEND = "CloudStorageBackend"

    assert entity.last_change == first_time
    assert entity.first_seen == first_time
    assert entity.last_seen == second_time
