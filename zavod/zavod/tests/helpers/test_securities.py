from zavod.context import Context
from zavod.meta import Dataset
from zavod.helpers.securities import make_security


def test_make_security(testdataset1: Dataset):
    context = Context(testdataset1)

    entity = make_security(context, "US0378331005")
    assert entity is not None
    assert entity.id == "isin-US0378331005"
    assert entity.schema.name == "Security"
    assert entity.get("isin") == ["US0378331005"]
    assert entity.first("country") == "us"

    entity = make_security(context, "XS1234567896")
    assert entity is not None
    assert entity.id == "isin-XS1234567896"
    assert entity.schema.name == "Security"
    assert entity.get("isin") == ["XS1234567896"]
    assert not len(entity.get("country"))

    entity = make_security(context, "DE1234567896")
    assert entity is not None
    assert entity.id == "isin-DE1234567896"
    assert entity.first("country") == "de"

    # Input normalization: lowercase + surrounding whitespace
    entity = make_security(context, "  us0378331005  ")
    assert entity is not None
    assert entity.id == "isin-US0378331005"

    # Invalid / unparseable input must not mint an entity
    assert make_security(context, "N/A") is None
    assert make_security(context, "US0378331003") is None
    context.close()
