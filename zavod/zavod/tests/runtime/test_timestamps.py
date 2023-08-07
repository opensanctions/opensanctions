from datetime import datetime, timedelta
from shutil import copyfile, rmtree
from tempfile import mkdtemp
from pathlib import Path

from zavod import settings
from zavod.meta import Dataset
from zavod.runner import run_dataset
from zavod.archive import iter_dataset_statements
from zavod.runtime.timestamps import TimeStampIndex


def test_timestamps(vdataset: Dataset):
    run_dataset(vdataset)

    prev_time = str(settings.RUN_TIME_ISO)
    stmts = list(iter_dataset_statements(vdataset))
    for stmt in stmts:
        assert stmt.first_seen == prev_time

    dt = datetime.utcnow().replace(microsecond=0) + timedelta(days=1)
    default = dt.isoformat(sep="T", timespec="seconds")

    index = TimeStampIndex(dataset=vdataset)
    index.index(stmts)
    assert index.get("test", default) == default
    for stmt in stmts:
        assert index.get(stmt.id, default) != ""
        assert index.get(stmt.id, default) == prev_time

    assert "TimeStampIndex" in repr(index), repr(index)


def test_backfill(vdataset: Dataset):   
    dataset_path = settings.DATA_PATH / "datasets" / vdataset.name
    rmtree(dataset_path, ignore_errors=True)
    
    settings.ARCHIVE_BACKEND = "FilesystemBackend"
    settings.ARCHIVE_PATH = Path(mkdtemp())
    prev_time = settings.RUN_TIME_ISO
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
    
    # Restore settings
    settings.ARCHIVE_BACKEND = "CloudStorageBackend"

    stmts = list(iter_dataset_statements(vdataset))
    index = TimeStampIndex.build(dataset=vdataset)
    for stmt in stmts:
        assert index.get(stmt.id, second_time) != ""
        assert index.get(stmt.id, second_time) == prev_time