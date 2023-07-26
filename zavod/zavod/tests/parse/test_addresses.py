import pytest

from zavod.context import Context
from zavod.parse.addresses import make_address, apply_address


def test_make_address_helper(vcontext: Context):
    addr = make_address(
        vcontext,
        street="123 Main St",
        city="Springfield",
        postal_code="12345",
        country="United States of America",
    )
    assert "us" in addr.get("country")
    assert addr.first("full") == "123 Main St, Springfield, 12345"
    assert addr.id is not None
    assert addr.id.startswith("addr-")

    addr = make_address(
        vcontext,
        full="123 Main Street, Springfield, 12345",
        street="123 Main St",
        city="Springfield",
        postal_code="12345",
        country="United States of America",
    )
    full = addr.first("full")
    assert full is not None
    assert "Street" in full

    empty = make_address(vcontext)
    assert empty.id is None

    person = vcontext.make("Person")
    person.id = "jeff"

    apply_address(vcontext, person, addr)
    assert person.first("addressEntity") == addr.id
    assert person.first("country") == "us"

    with pytest.raises(AssertionError):
        other = vcontext.make("Company")
        other.id = "corp"
        apply_address(vcontext, person, other)

    country = make_address(vcontext, country="Mozambique")
    assert country.id is None
    apply_address(vcontext, person, country)
    assert "mz" in person.get("country")


def test_make_address_cleaning(vcontext: Context):
    addr = make_address(vcontext, full="Congo")
    assert "DR Congo" in addr.get("full")

    addr = make_address(vcontext, city="Moscou")
    assert addr.first("full") == "Moscow"
