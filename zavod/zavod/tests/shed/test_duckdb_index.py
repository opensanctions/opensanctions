from pathlib import Path
from tempfile import mkdtemp
from nomenklatura import CompositeEntity

from zavod.integration.duckdb_index import DuckDBIndex
from zavod.crawl import crawl_dataset
from zavod.integration import get_resolver
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


def test_pairs(testdataset_dedupe: Dataset):
    crawl_dataset(testdataset_dedupe)
    data_dir = Path(mkdtemp()).resolve()
    resolver = get_resolver()
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
    assert pairs[0] == (
        ("matching-john-smith-us", "matching-john-smith-uk"),
        24.0,
    ), pairs[0]

    # Almost exact name, same country, penalised by longer name Term Frequency
    assert pairs[1][0] == (
        "matching-john-smith-us",
        "matching-john-gregory-smith-us",
    ), pairs[1]
    assert 5 < pairs[1][1] < 6, pairs[1]

    # One token matching scores poorly
    bond = scores[("matching-john-smith-uk", "matching-james-bond-uk-007")]
    assert bond == 2.0, bond


def test_match(testdataset1: Dataset, testdataset_dedupe: Dataset):
    crawl_dataset(testdataset_dedupe)
    data_dir = Path(mkdtemp()).resolve()
    resolver = get_resolver()
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
    assert entity_matches["id-bond"][0][1] == 6.0, entity_matches["id-bond"]

    john_matches = entity_matches["id-john"]
    assert len(john_matches) == 5, john_matches

    # Exact name matches full name and name parts -> top score
    assert john_matches[0] == ("matching-john-smith-us", 23.0), john_matches[0]
    # Unboosted differing token scores slightly lower
    assert john_matches[1] == ("matching-john-smith-uk", 22.0), john_matches[0]
