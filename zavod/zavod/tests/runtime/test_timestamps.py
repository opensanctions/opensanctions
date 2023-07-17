from datetime import datetime, timedelta

from zavod import settings
from zavod.meta import Dataset
from zavod.runner import run_dataset
from zavod.archive import iter_dataset_statements
from zavod.runtime.timestamps import TimeStampIndex


def test_enrich_process(vdataset: Dataset):
    run_dataset(vdataset)

    stmts = list(iter_dataset_statements(vdataset))
    dt = datetime.utcnow().replace(microsecond=0) + timedelta(days=1)
    prev_time = settings.RUN_TIME_ISO
    settings.RUN_TIME_ISO = dt.isoformat(sep="T", timespec="seconds")

    index = TimeStampIndex(dataset=vdataset)
    index.index(stmts)
    assert index.get("test") == settings.RUN_TIME_ISO
    for stmt in stmts:
        assert index.get(stmt.id) == prev_time

    assert "TimeStampIndex" in repr(index), repr(index)
