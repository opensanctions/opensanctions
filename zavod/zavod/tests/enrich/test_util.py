from followthemoney import model

from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.runner.util import (
    check_publishability,
    endpoint_ids,
    is_supporting_schema,
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
    publishability = check_publishability([address], FakeView([address]), frozenset())
    assert should_promote(address, publishability)
    article = make(testdataset1, "Article", "article", title=["Scoop"])
    publishability = check_publishability([article], FakeView([article]), frozenset())
    assert should_promote(article, publishability)
    sanction = make(testdataset1, "Sanction", "sanc", reason=["bad"])
    publishability = check_publishability([sanction], FakeView([sanction]), frozenset())
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


def test_check_publishability(testdataset1: Dataset) -> None:
    tagged = make(testdataset1, "Person", "tagged", topics=["crime.boss"])
    untagged = make(testdataset1, "Person", "untagged", name=["Jane"])
    article = make(testdataset1, "Article", "article", title=["Scoop"])
    ownership = make(
        testdataset1, "Ownership", "own", owner=["tagged"], asset=["untagged"]
    )
    documentation = make(
        testdataset1, "Documentation", "doc", entity=["tagged"], document=["article"]
    )

    view = FakeView([tagged, untagged, article])
    expanded = [tagged, untagged, article, ownership, documentation]
    publishable = check_publishability(expanded, view, ENRICH_TOPICS)

    # The article is looked up as a Documentation endpoint and is publishable
    # as a supporting schema despite carrying no topic.
    assert publishable == {"tagged": True, "untagged": False, "article": True}

    assert should_promote(tagged, publishable)
    assert not should_promote(untagged, publishable)
    assert should_promote(article, publishable)
    # The edge to the supporting article follows the article ...
    assert should_promote(documentation, publishable)
    # ... but the edge to the lateral untagged person still drops.
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
