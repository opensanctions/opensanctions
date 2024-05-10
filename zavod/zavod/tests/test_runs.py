import os
from zavod.runs import RunID


def test_run_id():
    runid = RunID.new()
    assert runid.id.startswith(runid.dt.strftime("%Y%m%d%H%M%S"))
    assert len(runid.tag) == 4
    assert len(runid.id) == 19
    assert str(runid) == runid.id
    assert repr(runid) == f"RunID({runid.id})"
    runid2 = RunID.new()
    assert runid2.id != runid.id

    os.environ["ZAVOD_RUN_ID"] = runid.id
    runid3 = RunID.from_env("ZAVOD_RUN_ID")
    assert runid3.id == runid.id

    runid4 = RunID.from_env("ZAVOD_RUN2222_ID")
    assert runid4.id != runid2.id
