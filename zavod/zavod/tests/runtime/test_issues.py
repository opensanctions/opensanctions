from zavod.logs import configure_logging
from zavod.archive import ISSUES_LOG
from zavod.context import Context
from zavod.meta import Dataset


def test_issue_logger(vdataset: Dataset):
    configure_logging()
    context = Context(vdataset)
    context.begin(clear=True)
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
    issues = list(context.issues.all())
    assert len(issues) == 2
    assert context.issues.by_level()["error"] == 1
    assert context.issues.by_level()["warning"] == 1
    assert issues[0]["level"] == "warning"
    assert issues[0]["data"]["foo"] == "bar"
    assert issues[0]["data"]["person"] == "Person"
    assert issues[0]["data"]["path"].endswith(ISSUES_LOG)

    context = Context(vdataset)
    context.begin(clear=True)
    assert len(list(context.issues.all())) == 0
