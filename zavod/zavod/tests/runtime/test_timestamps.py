from datetime import datetime, timedelta

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

    settings.RUN_TIME_ISO = dt.isoformat(sep="T", timespec="seconds")

    index = TimeStampIndex(dataset=vdataset)
    index.index(stmts)
    assert index.get("test") == settings.RUN_TIME_ISO
    for stmt in stmts:
        assert index.get(stmt.id) != ""
        assert index.get(stmt.id) == prev_time

    assert "TimeStampIndex" in repr(index), repr(index)
