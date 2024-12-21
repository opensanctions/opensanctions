from datetime import timedelta
from shutil import copyfile
from rigour.time import utc_now

from zavod import settings
from zavod.meta import Dataset
from zavod.crawl import crawl_dataset
from zavod.archive import iter_dataset_statements
from zavod.runtime.timestamps import TimeStampIndex


def test_timestamps(testdataset1: Dataset):
    crawl_dataset(testdataset1)

    prev_time = str(settings.RUN_TIME_ISO)
    stmts = list(iter_dataset_statements(testdataset1))
    for stmt in stmts:
        assert stmt.first_seen == prev_time

    dt = utc_now().replace(microsecond=0) + timedelta(days=1)
    default = dt.isoformat(sep="T", timespec="seconds")

    index = TimeStampIndex(dataset=testdataset1)
    index.index(stmts)
    stamps = index.get("osv-john-doe")
    assert len(stamps), stamps
    assert stamps.get("test", default) == default
    for stmt in stmts:
        if stmt.entity_id != "osv-john-doe":
            continue
        assert stamps.get(stmt.id, default) != ""
        assert stamps.get(stmt.id, default) == prev_time

    assert "TimeStampIndex" in repr(index), repr(index)


def test_backfill(testdataset1: Dataset):
    prev_time = settings.RUN_TIME_ISO
    crawl_dataset(testdataset1)

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
    crawl_dataset(testdataset1)

    stmts = list(iter_dataset_statements(testdataset1))
    index = TimeStampIndex.build(dataset=testdataset1)
    stamps = index.get("osv-john-doe")
    assert len(stamps), stamps
    for stmt in stmts:
        if stmt.entity_id != "osv-john-doe":
            continue
        assert stamps.get(stmt.id, second_time) != ""
        assert stamps.get(stmt.id, second_time) == prev_time
