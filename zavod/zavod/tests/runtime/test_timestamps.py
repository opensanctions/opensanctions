from datetime import timedelta

import pytest
from followthemoney.dataset import Version
from rigour.time import utc_now

from zavod import settings
from zavod.meta import Dataset
from zavod.crawl import crawl_dataset
from zavod.runtime.manifest import Manifest
from zavod.runtime.timestamps import TimeStampIndex
from zavod.tests.util import run_dataset


def test_timestamps(testdataset1: Dataset):
    version = settings.RUN_VERSION
    crawl_dataset(testdataset1, version)

    prev_time = str(settings.RUN_TIME_ISO)
    manifest = Manifest.load_artifact(testdataset1, version)
    stmts = list(manifest.statements())
    for stmt in stmts:
        assert stmt.first_seen == prev_time

    dt = utc_now().replace(microsecond=0) + timedelta(days=1)
    default = dt.isoformat(sep="T", timespec="seconds")

    index = TimeStampIndex(testdataset1, version)
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


def test_backfill(testdataset1: Dataset, monkeypatch: pytest.MonkeyPatch):
    prev_time = settings.RUN_TIME_ISO
    # Run the dataset once to output statements.pack
    # Publish to archive statements and make them discoverable by the next run.
    first_version = run_dataset(testdataset1)

    # A second run, a day later: the run version and the derived run time move
    # together.
    next_dt = first_version.dt + timedelta(days=1)
    second_version = Version.from_string(next_dt.strftime("%Y%m%d%H%M%S") + "-bbb")
    monkeypatch.setattr(settings, "RUN_VERSION", second_version)
    monkeypatch.setattr(settings, "RUN_TIME", second_version.dt)
    monkeypatch.setattr(
        settings,
        "RUN_TIME_ISO",
        second_version.dt.isoformat(sep="T", timespec="seconds"),
    )
    monkeypatch.setattr(settings, "RUN_DATE", second_version.dt.date().isoformat())
    second_time = settings.RUN_TIME_ISO
    crawl_dataset(testdataset1, second_version)

    manifest = Manifest.load_artifact(testdataset1, second_version)
    stmts = list(manifest.statements())
    index = TimeStampIndex.build(dataset=testdataset1)
    stamps = index.get("osv-john-doe")
    assert len(stamps), stamps
    for stmt in stmts:
        if stmt.entity_id != "osv-john-doe":
            continue
        assert stamps.get(stmt.id, second_time) != ""
        assert stamps.get(stmt.id, second_time) == prev_time
