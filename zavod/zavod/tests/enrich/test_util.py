from followthemoney import model
from structlog.testing import capture_logs

from zavod.context import Context
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.runner.util import (
    check_publishability,
    endpoint_ids,
    is_supporting_schema,
    prune_unpublishable_references,
    should_promote,
)

ENRICH_TOPICS = frozenset({"reg.warn", "crime.boss"})


class FakeLinker:
    def get_canonical(self, entity_id: str) -> str:
        return entity_id


class FakeStore:
    linker = FakeLinker()


class FakeView:
    """Stands in for zavod.store.View in check_publishability lookups."""

    def __init__(self, entities: list[Entity]):
        self.store = FakeStore()
        self._entities = {e.id: e for e in entities}

    def get_entity(self, entity_id: str) -> Entity | None:
        return self._entities.get(entity_id)


def make(dataset: Dataset, schema: str, id: str, **props: list[str]) -> Entity:
    return Entity.from_data(dataset, {"schema": schema, "id": id, "properties": props})


def test_is_supporting_schema() -> None:
    supporting = [
        "Address",
        "Article",
        "Image",
        "PlainText",
        "Note",
        "Sanction",
        "Passport",
        "Identification",
    ]
    for name in supporting:
        assert is_supporting_schema(model.get(name)), name
    risk_targets = [
        "Person",
        "Company",
        "Organization",
        "LegalEntity",
        "Security",
        "Position",
        "CryptoWallet",
    ]
    for name in risk_targets:
        assert not is_supporting_schema(model.get(name)), name


def test_endpoint_ids(testdataset1: Dataset) -> None:
    ownership = make(testdataset1, "Ownership", "own", owner=["per"], asset=["com"])
    assert endpoint_ids(ownership) == {"per", "com"}
    person = make(testdataset1, "Person", "per", name=["Jane"])
    assert endpoint_ids(person) == set()


def test_should_promote_non_edges(testdataset1: Dataset) -> None:
    # Supporting schemata publish without any lookup entry.
    address = make(testdataset1, "Address", "addr", full=["1 Main St"])
    publishability = check_publishability([address], FakeView([]), frozenset())
    assert should_promote(address, publishability)
    article = make(testdataset1, "Article", "article", title=["Scoop"])
    publishability = check_publishability([article], FakeView([]), frozenset())
    assert should_promote(article, publishability)
    sanction = make(testdataset1, "Sanction", "sanc", reason=["bad"])
    publishability = check_publishability([sanction], FakeView([]), frozenset())
    assert should_promote(sanction, publishability)

    # Risk targets publish iff their lookup found a matching topic.
    person = make(testdataset1, "Person", "per", name=["Jane"])
    assert should_promote(person, {"per": True})
    assert not should_promote(person, {"per": False})
    assert not should_promote(person, {})


def test_should_promote_edges(testdataset1: Dataset) -> None:
    ownership = make(testdataset1, "Ownership", "own", owner=["per"], asset=["com"])
    assert should_promote(ownership, {"per": True, "com": True})
    assert not should_promote(ownership, {"per": True, "com": False})
    assert not should_promote(ownership, {"per": True})

    # An edge with no endpoint values never publishes.
    dangling = make(testdataset1, "Ownership", "own2", role=["shareholder"])
    assert not should_promote(dangling, {})

    # Documentation is an edge like any other: publishable iff its endpoints are,
    # regardless of it being an Interval subclass.
    documentation = make(
        testdataset1, "Documentation", "doc", entity=["per"], document=["art"]
    )
    assert should_promote(documentation, {"per": True, "art": True})
    assert not should_promote(documentation, {"per": True, "art": False})
    assert not should_promote(documentation, {"per": True})


def test_check_publishability(testdataset1: Dataset) -> None:
    tagged = make(testdataset1, "Person", "tagged", topics=["crime.boss"])
    untagged = make(testdataset1, "Person", "untagged", name=["Jane"])
    ownership = make(
        testdataset1, "Ownership", "own", owner=["tagged"], asset=["untagged"]
    )

    view = FakeView([tagged, untagged])
    expanded = [tagged, untagged, ownership]
    publishable = check_publishability(expanded, view, ENRICH_TOPICS)
    assert publishable == {"tagged": True, "untagged": False}

    assert should_promote(tagged, publishable)
    assert not should_promote(untagged, publishable)
    # The edge to the lateral untagged person drops.
    assert not should_promote(ownership, publishable)


def test_check_publishability_supporting_absent_from_view(
    testdataset1: Dataset,
) -> None:
    """Supporting entities come from the target dataset via expansion and are
    typically absent from the subject view; they and the edges reaching them
    must still publish (e.g. ext_us_ofac_press_releases Articles)."""
    tagged = make(testdataset1, "Person", "tagged", topics=["crime.boss"])
    article = make(testdataset1, "Article", "article", title=["Scoop"])
    documentation = make(
        testdataset1, "Documentation", "doc", entity=["tagged"], document=["article"]
    )

    # The subject view knows nothing about the article.
    view = FakeView([tagged])
    expanded = [tagged, article, documentation]
    publishable = check_publishability(expanded, view, ENRICH_TOPICS)
    assert publishable == {"tagged": True, "article": True}

    assert should_promote(article, publishable)
    assert should_promote(documentation, publishable)


def test_check_publishability_missing_endpoint(testdataset1: Dataset) -> None:
    tagged = make(testdataset1, "Person", "tagged", topics=["crime.boss"])
    edge = make(
        testdataset1, "Documentation", "doc", entity=["tagged"], document=["ghost"]
    )
    view = FakeView([tagged])
    publishable = check_publishability([edge], view, ENRICH_TOPICS)
    assert publishable == {"tagged": True, "ghost": False}
    assert not should_promote(edge, publishable)


def test_prune_unpublishable_references(vcontext: Context) -> None:
    security = make(
        vcontext.dataset, "Security", "sec", name=["AAA"], issuer=["pub", "unpub"]
    )
    publishable = {"sec": True, "pub": True, "unpub": False}
    with capture_logs() as cap_logs:
        pruned = prune_unpublishable_references(vcontext, security, publishable)
    assert security.get("issuer") == ["pub"]
    # The removed pair is returned so the caller can re-emit it as external.
    assert [(prop.name, ref) for prop, ref in pruned] == [("issuer", "unpub")]
    assert {
        "event": "Demoting reference to unpublishable entity to external",
        "log_level": "info",
        "entity_id": "sec",
        "prop": "issuer",
        "ref": "unpub",
    } in cap_logs

    # Edges are left untouched: promotion already guarantees their endpoints
    # are publishable, and unpublishable edges aren't published at all.
    ownership = make(
        vcontext.dataset, "Ownership", "own", owner=["pub"], asset=["unpub"]
    )
    with capture_logs() as cap_logs:
        pruned = prune_unpublishable_references(vcontext, ownership, publishable)
    assert pruned == []
    assert ownership.get("asset") == ["unpub"]
    assert cap_logs == []
