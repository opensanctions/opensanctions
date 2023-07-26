from zavod.context import Context
from zavod.parse.identification import make_identification


def test_make_identification_helper(vcontext: Context):
    person = vcontext.make("Person")
    person.id = "jeff"
    ident = make_identification(
        vcontext,
        person,
        number=None,
        doc_type="drivers license",
    )
    assert ident is None
    ident = make_identification(vcontext, person, number="1234567")
    assert ident is not None
    assert ident.schema.name == "Identification"
    assert ident.get("holder") == [person.id]
    assert ident.get("number") == ["1234567"]
    assert ident.id is not None
    ident = make_identification(
        vcontext,
        person,
        number="1234567",
        passport=True,
    )
    assert ident is not None
    assert ident.schema.name == "Passport"
