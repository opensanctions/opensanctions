from followthemoney import model
from nomenklatura.entity import CompositeEntity
from structlog.testing import capture_logs

from zavod.context import Context
from zavod.helpers import make_name, apply_name, split_comma_names


ENTITY = {
    "id": "bla",
    "schema": "Person",
}


def test_make_name():
    name = make_name(first_name="John", last_name="Doe")
    assert name == "John Doe"


def test_entity_name():
    entity = CompositeEntity.from_dict(model, ENTITY)
    apply_name(
        entity,
        first_name="John",
        second_name="Brandon",
        last_name="Doe",
        lang="eng",
    )
    assert entity.caption == "John Brandon Doe"
    for stmt in entity.get_statements("name"):
        assert stmt.lang == "eng"


def test_full_name():
    entity = CompositeEntity.from_dict(model, ENTITY)
    apply_name(
        entity,
        full="Zorro",
        first_name="John",
        second_name="Brandon",
        last_name="Doe",
        lang="eng",
    )
    assert entity.caption == "Zorro"
    for stmt in entity.get_statements("name"):
        assert stmt.lang == "eng"


def test_alias_name():
    entity = CompositeEntity.from_dict(model, ENTITY)
    apply_name(
        entity,
        first_name="John",
        second_name="Brandon",
        last_name="Doe",
        lang="eng",
        alias=True,
    )
    assert entity.get("name") == []
    assert "John Brandon Doe" in entity.get("alias")
    apply_name(
        entity,
        first_name="Johnny",
        last_name="Doe",
        lang="eng",
        is_weak=True,
    )
    assert "Johnny Doe" in entity.get("weakAlias")


def test_company_name(vcontext: Context):
    entity = vcontext.make("Company")
    entity.id = "bla"
    apply_name(
        entity,
        first_name="Hansen",
        last_name="Enterprises",
        lang="eng",
        alias=True,
        quiet=True,
    )
    assert entity.get("name") == []
    assert "Hansen Enterprises" in entity.get("alias")
    assert entity.get("firstName", quiet=True) == []


def test_split_comma_names(vcontext: Context, caplog):
    assert split_comma_names(vcontext, "") == []
    assert split_comma_names(vcontext, "John Smith") == ["John Smith"]
    assert split_comma_names(vcontext, "Smith, John") == ["Smith, John"]
    assert split_comma_names(vcontext, "John Smith, Jr.") == ["John Smith Jr."]
    assert split_comma_names(vcontext, "A B C, D E F") == ["A B C", "D E F"]
    assert split_comma_names(vcontext, "A B C, Ltd., D E F, Inc.") == [
        "A B C Ltd.",
        "D E F Inc.",
    ]
    assert split_comma_names(vcontext, "A B and C, D E F, John Lookups Smith") == [
        "A B and C",
        "D E F",
        "John Lookups Smith",
    ]

    # Not in lookups

    # Shouldn't be split
    assert split_comma_names(vcontext, "A, B and C Ltd.") == ["A, B and C Ltd."]
    # Would have been nice if this could be split
    assert split_comma_names(
        vcontext,
        "songyan li, junhong xiong, k. ivan gothner and edward pazdro"
    ) == ["songyan li, junhong xiong, k. ivan gothner and edward pazdro"]
    
    with capture_logs() as cap_logs:
        # Would have been nice if this could be split
        assert split_comma_names(vcontext, "A B and C, D E F") == ["A B and C, D E F"]
    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert "warning: Not sure how to split on comma." in logs

    # We cannot not decide in a case like A and B Ltd. It can be the name of one company or two seperate entities
    with capture_logs() as cap_logs:
        assert split_comma_names(vcontext, "A and B Ltd.") == ["A", "B Ltd."]
    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert "warning: Not sure how to split on comma." in logs
