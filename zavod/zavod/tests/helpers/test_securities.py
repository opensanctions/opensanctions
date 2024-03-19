from zavod.context import Context
from zavod.meta import Dataset
from zavod.helpers.securities import make_security


def test_make_security(testdataset1: Dataset):
    context = Context(testdataset1)
    entity = make_security(context, "XS1234567890")
    assert entity.id == "isin-XS1234567890"
    assert entity.schema.name == "Security"
    assert entity.get("isin") == ["XS1234567890"]
    assert not len(entity.get("country"))

    entity = make_security(context, "DE1234567890")
    assert entity.id == "isin-DE1234567890"
    assert entity.schema.name == "Security"
    assert entity.first("country") == "de"
