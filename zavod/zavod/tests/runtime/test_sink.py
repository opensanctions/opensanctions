from followthemoney.statement import read_statements, PACK

from zavod import settings
from zavod.meta import Dataset
from zavod.context import Context


def test_dataset_sink(testdataset1: Dataset):
    context = Context(testdataset1)
    assert context.sink.path.is_relative_to(settings.DATA_PATH)
    entity = context.make("Person")
    entity.id = "foo"
    entity.add("name", "Foo")
    context.emit(entity)
    context.sink.close()
    assert context.sink.path.is_file()
    with open(context.sink.path, "rb") as fh:
        stmts = list(read_statements(fh, PACK))
        for stmt in stmts:
            assert stmt.dataset == testdataset1.name, stmt
            assert stmt.entity_id == "foo"
        assert len(stmts) == 2, stmts
        props = [s.prop for s in stmts]
        assert "id" in props, props
        assert "name" in props, props
    context.sink.clear()
    assert not context.sink.path.is_file()
