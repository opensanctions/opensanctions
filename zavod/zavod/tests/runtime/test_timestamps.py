from datetime import timedelta
from shutil import copyfile
from rigour.time import utc_now
from followthemoney.dataset import Version

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


def test_backfill(testdataset1: Dataset, monkeypatch):
    prev_time = settings.RUN_TIME_ISO
    crawl_dataset(testdataset1)

    archive_path = settings.ARCHIVE_PATH / "datasets/latest" / testdataset1.name
    archive_path.mkdir(parents=True, exist_ok=True)
    copyfile(
        settings.DATA_PATH
        / "datasets"
        / testdataset1.name
        / settings.RUN_VERSION.id
        / "statements.pack",
        archive_path / "statements.pack",
    )

    # A new run a day later is a new version. monkeypatch restores the process
    # globals afterwards, so bumping the run time doesn't leak into other tests.
    later = settings.RUN_TIME + timedelta(days=1)
    later_version = Version.from_string(later.strftime("%Y%m%d%H%M%S") + "-bbb")
    monkeypatch.setattr(settings, "RUN_VERSION", later_version)
    monkeypatch.setattr(settings, "RUN_TIME", later)
    monkeypatch.setattr(
        settings, "RUN_TIME_ISO", later.isoformat(sep="T", timespec="seconds")
    )
    monkeypatch.setattr(settings, "RUN_DATE", later.date().isoformat())
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
