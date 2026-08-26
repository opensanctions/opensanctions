from zavod.context import Context
from zavod.meta import Dataset
from zavod.helpers.securities import make_security


def test_make_security(testdataset1: Dataset):
    context = Context(testdataset1)
    entity = make_security(context, "XS1234567896")
    assert entity is not None
    assert entity.id == "isin-XS1234567896"
    assert entity.schema.name == "Security"
    assert entity.get("isin") == ["XS1234567896"]
    assert not len(entity.get("country"))

    entity = make_security(context, "DE0005140008")
    assert entity is not None
    assert entity.id == "isin-DE0005140008"
    assert entity.schema.name == "Security"
    assert entity.first("country") == "de"
    context.close()


def test_make_security_normalizes(testdataset1: Dataset):
    context = Context(testdataset1)
    # Whitespace, separators and case must not produce distinct global IDs.
    for raw in [" US0378331005 ", "us0378331005", "US 0378 3310 05"]:
        entity = make_security(context, raw)
        assert entity is not None, raw
        assert entity.id == "isin-US0378331005"
        assert entity.get("isin") == ["US0378331005"]
        assert entity.first("country") == "us"
    context.close()


def test_make_security_invalid(testdataset1: Dataset):
    context = Context(testdataset1)
    # Placeholders, empty values and checksum failures must not mint an ID.
    for raw in ["N/A", "", "   ", "US0378331006", "DE1234567890", "not-an-isin"]:
        assert make_security(context, raw) is None, raw
    context.close()
