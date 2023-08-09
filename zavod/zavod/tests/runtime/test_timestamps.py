from datetime import datetime, timedelta
from shutil import copyfile

from zavod import settings
from zavod.meta import Dataset
from zavod.runner import run_dataset
from zavod.archive import iter_dataset_statements
from zavod.runtime.timestamps import TimeStampIndex


def test_timestamps(testdataset1: Dataset):
    run_dataset(testdataset1)

    prev_time = str(settings.RUN_TIME_ISO)
    stmts = list(iter_dataset_statements(testdataset1))
    for stmt in stmts:
        assert stmt.first_seen == prev_time

    dt = datetime.utcnow().replace(microsecond=0) + timedelta(days=1)
    default = dt.isoformat(sep="T", timespec="seconds")

    index = TimeStampIndex(dataset=testdataset1)
    index.index(stmts)
    assert index.get("test", default) == default
    for stmt in stmts:
        assert index.get(stmt.id, default) != ""
        assert index.get(stmt.id, default) == prev_time

    assert "TimeStampIndex" in repr(index), repr(index)


def test_backfill(testdataset1: Dataset):
    prev_time = settings.RUN_TIME_ISO
    run_dataset(testdataset1)

    archive_path = settings.ARCHIVE_PATH / "datasets/latest" / testdataset1.name
    archive_path.mkdir(parents=True, exist_ok=True)
    copyfile(
        settings.DATA_PATH / "datasets" / testdataset1.name / "statements.pack",
        archive_path / "statements.pack",
    )

    settings.RUN_TIME = settings.RUN_TIME + timedelta(days=1)
    settings.RUN_TIME_ISO = settings.RUN_TIME.isoformat(sep="T", timespec="seconds")
    settings.RUN_DATE = settings.RUN_TIME.date().isoformat()
    second_time = settings.RUN_TIME_ISO
    run_dataset(testdataset1)

    stmts = list(iter_dataset_statements(testdataset1))
    index = TimeStampIndex.build(dataset=testdataset1)
    for stmt in stmts:
        assert index.get(stmt.id, second_time) != ""
        assert index.get(stmt.id, second_time) == prev_time
