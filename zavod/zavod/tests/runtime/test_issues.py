import json
import logging

from followthemoney.dataset import Version
from rigour.time import iso_datetime

from zavod import settings
from zavod.archive import ISSUES_FILE, dataset_artifact_path
from zavod.meta import Dataset
from zavod.tests.util import make_context


def test_issue_logger(testdataset1: Dataset, logger: logging.Logger):
    issues_path = dataset_artifact_path(
        testdataset1.name, settings.RUN_VERSION, ISSUES_FILE
    )
    context = make_context(testdataset1)
    context.begin()
    assert not issues_path.exists()
    entity = context.make("Person")
    entity.id = "guy"
    entity.add("name", "Some Guy")
    context.log.warn(
        "This is a warning",
        foo="bar",
        person=entity.schema,
        path=issues_path,
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
    assert issues[0]["data"]["path"].endswith(ISSUES_FILE)

    issues_path.unlink()
    context.issues.export()
    assert issues_path.exists()
    with open(issues_path) as fh:
        data = json.load(fh)
        assert len(data["issues"]) == 2
        for issue in data["issues"]:
            assert issue["id"] is not None
            assert iso_datetime(issue["timestamp"]) is not None
            assert issue["level"] in ("warning", "error")
            assert issue["dataset"] == testdataset1.name

    # A fresh run is a fresh version, and starts with an empty issue log:
    context = make_context(testdataset1, Version.new("bbb"))
    context.begin()
    assert len(list(context.issues.all())) == 0
    context.close()
