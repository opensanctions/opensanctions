from pathlib import Path
from tempfile import mkdtemp

from normality import slugify
from nomenklatura import CompositeEntity, Resolver

from zavod.entity import Entity
from zavod.integration.duckdb_index import DuckDBIndex
from zavod.crawl import crawl_dataset
from zavod.meta.dataset import Dataset
from zavod.store import get_store


BOND = {
    "schema": "Person",
    "id": "id-bond",
    "properties": {"name": ["Secret McSecretface"], "idNumber": ["007"]},
}
JOHN = {
    "schema": "Person",
    "id": "id-john",
    "properties": {"name": ["John Smith"], "country": ["US"]},
}


def test_pairs(testdataset_dedupe: Dataset, resolver: Resolver[Entity]):
    crawl_dataset(testdataset_dedupe)
    data_dir = Path(mkdtemp()).resolve()
    store = get_store(testdataset_dedupe, resolver)
    store.sync(clear=True)
    view = store.view(testdataset_dedupe)

    index = DuckDBIndex(view, data_dir)
    index.build()
    pairs = list(
        ((left.id, right.id), score) for ((left, right), score) in index.pairs()
    )
    scores = dict(pairs)

    # Exact name matches full name and name parts -> top score
    assert pairs[0][0] == ("matching-john-smith-us", "matching-john-smith-uk"), pairs[0]
    assert pairs[0][1] > 23, pairs[0]

    # Almost exact name, same country, penalised by longer name Term Frequency
    assert pairs[1][0] == (
        "matching-john-smith-us",
        "matching-john-gregory-smith-us",
    ), pairs[1]
    assert 1 < pairs[1][1] < 6, pairs[1]

    # One token matching scores poorly
    bond = scores[("matching-john-smith-uk", "matching-james-bond-uk-007")]
    assert bond == 2.0, bond


def test_match(testdataset1: Dataset, testdataset_dedupe: Dataset, resolver: Resolver[Entity]):
    crawl_dataset(testdataset_dedupe)
    data_dir = Path(mkdtemp()).resolve()
    store = get_store(testdataset_dedupe, resolver)
    store.sync(clear=True)
    view = store.view(testdataset_dedupe)

    index = DuckDBIndex(view, data_dir)
    index.build()

    bond = CompositeEntity.from_data(testdataset1, BOND)
    index.add_matching_subject(bond)
    john = CompositeEntity.from_data(testdataset1, JOHN)
    index.add_matching_subject(john)

    entity_matches = {}
    for entity_id, matches in index.matches():
        entity_matches[entity_id] = [(match.id, score) for match, score in matches]

    assert len(entity_matches["id-bond"]) == 1
    assert entity_matches["id-bond"][0][0] == "matching-james-bond-uk-007"
    assert entity_matches["id-bond"][0][1] > 0, entity_matches["id-bond"]

    john_matches = entity_matches["id-john"]
    assert len(john_matches) == 5, john_matches

    # Exact name matches full name and name parts -> top score
    assert john_matches[0][0] == "matching-john-smith-us", john_matches[0]
    assert john_matches[0][1] > 10, john_matches[0]
    # Unboosted differing token scores slightly lower
    assert john_matches[1][0] == "matching-john-smith-uk", john_matches[1]
    assert john_matches[0][1] > john_matches[1][1], john_matches[1]


def test_stopwords(testdataset1: Dataset, resolver: Resolver[Entity]):
    def e(name: str) -> Entity:
        data = {
            "schema": "Person",
            "id": f"id-{slugify(name)}",
            "properties": {"name": [name]},
        }
        return resolver.apply(Entity.from_data(testdataset1, data))

    store = get_store(testdataset1, resolver)
    writer = store.writer()
    # 1 first name 5 times
    # 5 last names once each
    # 5 distinct full names
    # 11 tokens
    writer.add_entity(e("FirstA LastA"))
    writer.add_entity(e("FirstA LastB"))
    writer.add_entity(e("FirstA LastC"))
    writer.add_entity(e("FirstB LastD"))
    writer.add_entity(e("First LastE"))

    writer.flush()
    view = store.view(testdataset1)

    too_common_first_name = e("FirstA LastF")
    matching_last_name = e("FirstD LastA")

    # 15% most common tokens as stopwords -> ignore FirstA

    data_dir = Path(mkdtemp()).resolve()
    index = DuckDBIndex(view, data_dir, {"stopwords_pct": 15})
    index.build()

    index.add_matching_subject(too_common_first_name)
    index.add_matching_subject(matching_last_name)
    entity_matches = {}
    for entity_id, matches in index.matches():
        entity_matches[entity_id] = [(match.id, score) for match, score in matches]

    assert too_common_first_name.id not in entity_matches
    assert len(entity_matches[matching_last_name.id]) == 1
    assert entity_matches[matching_last_name.id][0][0] == "id-firsta-lasta"

    # 5% most common tokens as stopwords -> ignore nothing

    data_dir = Path(mkdtemp()).resolve()
    index = DuckDBIndex(view, data_dir, {"stopwords_pct": 5})
    index.build()

    index.add_matching_subject(too_common_first_name)
    index.add_matching_subject(matching_last_name)

    entity_matches = {}
    for entity_id, matches in index.matches():
        entity_matches[entity_id] = [(match.id, score) for match, score in matches]

    assert len(entity_matches[too_common_first_name.id]) == 3
    assert len(entity_matches[matching_last_name.id]) == 1
