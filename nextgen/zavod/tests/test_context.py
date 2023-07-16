from zavod.context import Context
from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.runtime.loader import load_entry_point


def test_context_helpers(vdataset: Dataset):
    context = Context(vdataset)
    assert context.dataset == vdataset
    gen_id = "osv-e7bb50a8ac8cc89a980ae970de9d3b05af2973c2"
    assert context.make_id("john", "doe") == gen_id
    assert context.make_id("") is None
    assert context.make_slug("john", "doe") == "osv-john-doe"
    assert context.make_slug(None) is None

    entity = context.make("Person")
    assert isinstance(entity, Entity)
    assert entity.schema.name == "Person"

    assert context.lookup("plants", "banana").value == "Fruit"
    assert context.lookup_value("plants", "potato") == "Vegetable"
    assert context.lookup_value("plants", "stone") is None

    context.inspect(None)
    context.inspect("foo")


def test_run_dataset(vdataset: Dataset):
    context = Context(vdataset)
    func = load_entry_point(vdataset)
    func(context)
    context.close()
