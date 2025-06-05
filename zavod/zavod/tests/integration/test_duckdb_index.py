from pathlib import Path
from tempfile import mkdtemp

from nomenklatura import Resolver
from normality import slugify

from zavod.crawl import crawl_dataset
from zavod.entity import Entity
from zavod.integration.duckdb_index import DEFAULT_FIELD_STOPWORDS_PCT, DuckDBIndex
from zavod.integration.tokenizer import NAME_PART_FIELD, PHONETIC_FIELD
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
NO_STOPWORDS = DEFAULT_FIELD_STOPWORDS_PCT.copy()
for field in NO_STOPWORDS:
    NO_STOPWORDS[field] = 0.0


def test_pairs(testdataset_dedupe: Dataset, resolver: Resolver[Entity]):
    crawl_dataset(testdataset_dedupe)
    data_dir = Path(mkdtemp()).resolve()
    store = get_store(testdataset_dedupe, resolver)
    store.sync(clear=True)
    view = store.view(testdataset_dedupe)

    index = DuckDBIndex(view, data_dir, {"stopwords_pct": NO_STOPWORDS})
    index.build()
    pairs = list(
        ((left.id, right.id), score) for ((left, right), score) in index.pairs()
    )
    scores = dict(pairs)

    # Exact name matches full name and name parts -> top score
    assert pairs[0][0] == ("matching-john-smith-us", "matching-john-smith-uk"), pairs[0]
    assert pairs[0][1] > 14, pairs[0]

    # Almost exact name, same country, penalised by longer name Term Frequency
    assert pairs[1][0] == (
        "matching-john-smith-us",
        "matching-john-gregory-smith-us",
    ), pairs[1]
    assert 1 < pairs[1][1] < 12, pairs[1]

    # One token matching scores poorly
    bond = scores[("matching-john-smith-uk", "matching-james-bond-uk-007")]
    assert bond == 2.0, bond


def test_match(
    testdataset1: Dataset, testdataset_dedupe: Dataset, resolver: Resolver[Entity]
):
    crawl_dataset(testdataset_dedupe)
    data_dir = Path(mkdtemp()).resolve()
    store = get_store(testdataset_dedupe, resolver)
    store.sync(clear=True)
    view = store.view(testdataset_dedupe)

    index = DuckDBIndex(view, data_dir, {"stopwords_pct": NO_STOPWORDS})
    index.build()

    bond = Entity.from_data(testdataset1, BOND)
    john = Entity.from_data(testdataset1, JOHN)

    # There's a company in the data with the same name as the person
    assert view.get_entity("matching-john-smith-inc-us").schema.is_a("Company")
    entity_matches = {}
    for entity_id, matches in index.match_entities([bond, john]):
        entity_matches[entity_id] = []
        for match, score in matches:
            entity_matches[entity_id].append((match.id, score))
            # The company didn't match
            assert not view.get_entity(match.id).schema.is_a("Company"), (
                entity_id,
                match.id,
            )

    assert len(entity_matches["id-bond"]) == 1
    assert entity_matches["id-bond"][0][0] == "matching-james-bond-uk-007"
    assert entity_matches["id-bond"][0][1] > 0, entity_matches["id-bond"]

    john_matches = entity_matches["id-john"]
    assert len(john_matches) == 5, john_matches

    # Exact name matches full name and name parts -> top score
    assert john_matches[0][0] == "matching-john-smith-us", john_matches[0]
    assert john_matches[0][1] > 8, john_matches[0]
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

    # FirstA 3 times = 1 token
    # FirstB, FirstC once = 2 tokens
    # 5 last names once each = 5 tokens
    # total: 8 name part tokens
    # phonemes of these = 4 tokens
    #
    # To treat FirstA and its phoneme as stopwords:
    # we need a name part stopword pct between 1/8 = 12.5% and 2/8 = 25%
    # we need a phoneme stopword pct between 1/4 = 25% and 2/4 = 50%
    writer.add_entity(e("FirstA LastA"))
    writer.add_entity(e("FirstA LastB"))
    writer.add_entity(e("FirstA LastC"))
    writer.add_entity(e("FirstB LastD"))
    writer.add_entity(e("FirstC LastE"))

    writer.flush()
    view = store.view(testdataset1)

    too_common_first_name = e("FirstA LastF")
    # LastC because phoneme of LastC is uniquely LASTK here
    matching_last_name = e("FirstD LastC")

    # 18% most common tokens as stopwords -> ignore FirstA

    data_dir = Path(mkdtemp()).resolve()
    stopword_pcts = NO_STOPWORDS.copy()
    stopword_pcts[NAME_PART_FIELD] = 15.0
    stopword_pcts[PHONETIC_FIELD] = 30.0
    index = DuckDBIndex(view, data_dir, {"stopwords_pct": stopword_pcts})
    index.build()

    entity_matches = {}
    for entity_id, matches in index.match_entities(
        [too_common_first_name, matching_last_name]
    ):
        entity_matches[entity_id] = [(match.id, score) for match, score in matches]

    assert too_common_first_name.id not in entity_matches
    assert len(entity_matches[matching_last_name.id]) == 1
    assert entity_matches[matching_last_name.id][0][0] == "id-firsta-lastc"

    # 0% most common tokens as stopwords -> ignore nothing

    data_dir = Path(mkdtemp()).resolve()
    stopword_pcts[NAME_PART_FIELD] = 5.0
    stopword_pcts[PHONETIC_FIELD] = 5.0
    index = DuckDBIndex(view, data_dir, {"stopwords_pct": stopword_pcts})
    index.build()

    entity_matches = {}
    for entity_id, matches in index.match_entities(
        [too_common_first_name, matching_last_name]
    ):
        entity_matches[entity_id] = [(match.id, score) for match, score in matches]

    assert len(entity_matches[too_common_first_name.id]) == 3
    assert len(entity_matches[matching_last_name.id]) == 1
