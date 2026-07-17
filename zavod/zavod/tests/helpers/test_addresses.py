import pytest

from zavod.context import Context
from zavod.helpers.addresses import make_address, apply_address


def test_make_address_helper(vcontext: Context):
    addr = make_address(
        vcontext,
        street="123 Main St",
        city="Springfield",
        postal_code="12345",
        country="United States of America",
    )
    assert addr is not None, addr
    assert "us" in addr.get("country")
    expect = "123 Main St, Springfield, 12345, United States of America"
    assert addr.first("full") == expect
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
    assert addr is not None, addr
    full = addr.first("full")
    assert full is not None
    assert "Street" in full

    empty = make_address(vcontext)
    assert empty is None

    person = vcontext.make("Person")
    person.id = "jeff"

    apply_address(vcontext, person, addr)
    assert person.first("addressEntity") == addr.id
    assert person.first("country") == "us"
    assert "Main Street" in person.first("address")

    with pytest.raises(AssertionError):
        other = vcontext.make("Company")
        other.id = "corp"
        apply_address(vcontext, person, other)

    country = make_address(vcontext, country="Mozambique")
    assert country is not None
    assert not country.has("full")
    apply_address(vcontext, person, country)
    assert "mz" in person.get("country")


def test_make_address_state_not_duplicated(vcontext: Context):
    # state used to be folded into state_district as well, so templates that
    # render both slots printed the state twice ("PA, PA"):
    addr = make_address(vcontext, city=None, state="PA", country_code="us")
    assert addr is not None, addr
    assert addr.first("full") == "PA"

    addr = make_address(
        vcontext,
        street="1 Main St",
        state="California",
        region="Southern",
        country="United States",
    )
    assert addr is not None, addr
    full = addr.first("full")
    assert full is not None
    assert full.count("California") == 1, full
    assert "Southern" in full, full


def test_make_address_country_code_casing(vcontext: Context):
    # The country code is hashed into the address entity ID, so its casing
    # must be normalized for cross-dataset address dedup to work:
    lower = make_address(
        vcontext, street="123 Main St", city="Springfield", country_code="us"
    )
    upper = make_address(
        vcontext, street="123 Main St", city="Springfield", country_code="US"
    )
    assert lower is not None, lower
    assert upper is not None, upper
    assert lower.id == upper.id
    assert "us" in upper.get("country")


def test_make_address_cleaning(vcontext: Context):
    addr = make_address(vcontext, full="Congo")
    assert addr is not None, addr
    assert "DR Congo" in addr.get("full")

    addr = make_address(vcontext, city="Moscou")
    assert addr is not None, addr
    assert addr.first("full") == "Moscow"
