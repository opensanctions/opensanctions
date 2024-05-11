import os
import pytest
from zavod.runs import RunID, RunHistory


def test_run_id():
    runid = RunID.new()
    assert runid.id.startswith(runid.dt.strftime("%Y%m%d%H%M%S"))
    assert len(runid.tag) == 4
    assert len(runid.id) == 19
    assert str(runid) == runid.id
    assert repr(runid) == f"RunID({runid.id})"
    runid2 = RunID.new()
    assert runid2.id != runid.id
    assert hash(runid2) == hash(runid2.id)

    with pytest.raises(ValueError):
        RunID.from_string("foo")

    os.environ["ZAVOD_RUN_ID"] = runid.id
    runid3 = RunID.from_env("ZAVOD_RUN_ID")
    assert runid3.id == runid.id

    runid4 = RunID.from_env("ZAVOD_RUN2222_ID")
    assert runid4.id != runid2.id


def test_run_history():
    original = RunHistory([])
    assert original.latest is None
    assert original.to_json() == '{"items": []}'

    runid = RunID.new()
    history = original.append(runid)
    assert len(history) == 1
    assert len(original) == 0
    assert history.latest == runid
    assert history.to_json() == f'{{"items": ["{runid.id}"]}}'

    runid2 = RunID.new()
    history = history.append(runid2)
    assert history.latest == runid2
    assert history.to_json() == f'{{"items": ["{runid.id}", "{runid2.id}"]}}'
    assert len(list(history)) == 2

    other = RunHistory.from_json(history.to_json())
    assert other.latest == runid2

    for _ in range(10000):
        history = history.append(RunID.new())
        assert len(history) <= RunHistory.LENGTH

    history = RunHistory([runid, runid2])
    assert history.latest == runid2
    assert history.to_json() == f'{{"items": ["{runid.id}", "{runid2.id}"]}}'
