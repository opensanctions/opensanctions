from datetime import datetime, timedelta

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
