import pytest
from zavod.context import Context
from zavod.helpers.sanctions import make_sanction


def test_sanctions_helper(vcontext: Context):
    person = vcontext.make("Person")
    with pytest.raises(AssertionError):
        make_sanction(vcontext, person)

    person.id = "jeff"
    sanction = make_sanction(vcontext, person)
    assert "OpenSanctions" in sanction.get("authority")
    assert "jeff" in sanction.get("entity")

    sanction2 = make_sanction(vcontext, person)
    assert sanction.id == sanction2.id

    sanction3 = make_sanction(vcontext, person, key="other")
    assert sanction.id != sanction3.id
