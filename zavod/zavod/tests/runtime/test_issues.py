import json
from zavod.logs import configure_logging
from zavod.archive import ISSUES_LOG, ISSUES_FILE, dataset_resource_path
from zavod.context import Context
from zavod.meta import Dataset
from nomenklatura.util import iso_datetime


def test_issue_logger(vdataset: Dataset):
    configure_logging()
    issues_path = dataset_resource_path(vdataset.name, ISSUES_FILE)
    context = Context(vdataset)
    context.begin(clear=True)
    assert not issues_path.exists()
    entity = context.make("Person")
    entity.id = "guy"
    entity.add("name", "Some Guy")
    context.log.warn(
        "This is a warning",
        foo="bar",
        person=entity.schema,
        path=context.issues.path,
        entity=entity,
    )
    context.log.error("This is an error", qux="quux", entity="other")
    context.close()
    assert issues_path.exists()
    issues = list(context.issues.all())
    assert len(issues) == 2
    assert context.issues.by_level()["error"] == 1
    assert context.issues.by_level()["warning"] == 1
    assert issues[0]["level"] == "warning"
    assert issues[0]["data"]["foo"] == "bar"
    assert issues[0]["data"]["person"] == "Person"
    assert issues[0]["data"]["path"].endswith(ISSUES_LOG)

    issues_path.unlink()
    context.issues.export()
    assert issues_path.exists()
    with open(issues_path, "r") as fh:
        data = json.load(fh)
        assert len(data["issues"]) == 2
        for issue in data["issues"]:
            assert issue["id"] is not None
            assert iso_datetime(issue["timestamp"]) is not None
            assert issue["level"] in ("warning", "error")
            assert issue["dataset"] == vdataset.name

    context = Context(vdataset)
    context.begin(clear=True)
    assert len(list(context.issues.all())) == 0
