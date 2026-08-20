from datetime import timedelta
from rigour.time import utc_now

from zavod import settings
from zavod.exporters import export_dataset
from zavod.integration.dedupe import get_dataset_linker
from zavod.meta import Dataset
from zavod.crawl import crawl_dataset
from zavod.archive import iter_dataset_statements
from zavod.publish import publish_dataset
from zavod.runtime.timestamps import TimeStampIndex
from zavod.store import get_store


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
    # Run the dataset once to output statements.pack
    # Publish to archive statements and make them discoverable by the next run.
    linker = get_dataset_linker(testdataset1)
    crawl_dataset(testdataset1)
    store = get_store(testdataset1, linker)
    store.sync()
    view = store.view(testdataset1)
    export_dataset(testdataset1, view)
    publish_dataset(testdataset1)

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
