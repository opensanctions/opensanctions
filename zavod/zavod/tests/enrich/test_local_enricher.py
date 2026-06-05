import shutil
from copy import deepcopy

from nomenklatura.judgement import Judgement

from zavod import settings
from zavod.entity import Entity
from zavod.archive import clear_data_path
from zavod.context import Context
from zavod.crawl import crawl_dataset
from zavod.integration import get_dataset_linker
from zavod.integration.dedupe import get_resolver
from zavod.meta import Dataset
from zavod.runner.local_enricher import LocalEnricher, is_edge_internal
from zavod.store import get_store

DATASET_DATA = {
    "name": "test_enricher",
    "title": "An enrichment dataset",
    "config": {"cutoff": 0.5, "algorithm": "logic-v1"},
    "entry_point": "zavod.runner.local_enricher:enrich",
    "inputs": ["testdataset_enrich_subject"],
}
UMBRELLA_CORP = {
    "schema": "LegalEntity",
    "id": "xxx",
    "properties": {"name": ["Umbrella Corp."]},
}
JON_DOVER = {
    "schema": "Person",
    "id": "abc-jona-dova",  # Initially different from dataset
    "properties": {"name": ["Jonathan Dover"]},  # Different from dataset
}
AAA_USD_ISK = {
    "schema": "Security",
    "id": "us-aaa-usd-isk",
    "properties": {"name": ["AAA USD ISK"]},
}
AAA_BANK = {
    "schema": "Organization",
    "id": "us-aaa-inc",
    "properties": {"name": ["AAA Inc."]},
}


def make_enricher_dataset(dataset_data, target_dataset):
    dataset_data_copy = deepcopy(dataset_data)
    dataset_data_copy["full_dataset"] = target_dataset
    return Dataset.make(dataset_data_copy)


def load_enricher(context: Context, dataset_data, target_dataset: str):
    dataset = make_enricher_dataset(dataset_data, target_dataset)
    return LocalEnricher(dataset, context.cache, dataset.config)


def test_enrich_process(testdataset1: Dataset, testdataset_enrich_subject: Dataset):
    """We match and expand an entity with a similar name"""

    resolver = get_resolver()

    # Make a little subject dataset
    crawl_dataset(testdataset_enrich_subject)

    # Treat testdataset1 as the target dataset
    crawl_dataset(testdataset1)

    # Enrich the subject against the target
    resolver.begin()
    assert len(resolver.edges) == 0
    resolver.rollback()
    enricher_ds = make_enricher_dataset(DATASET_DATA, testdataset1.name)
    crawl_dataset(enricher_ds)
    assert enricher_ds.name == "test_enricher"

    resolver.begin()
    store = get_store(enricher_ds, resolver)
    store.sync(clear=True)
    internals = list(store.view(enricher_ds, external=False).entities())
    assert len(internals) == 0, internals
    externals = list(store.view(enricher_ds, external=True).entities())
    assert len(externals) == 1, externals
    store.close()

    # Judge a match candidate
    canon_id = resolver.decide("osv-umbrella-corp", "xxx", Judgement.POSITIVE)
    resolver.commit()

    # Enrich again, now with internals
    clear_data_path(enricher_ds.name)
    crawl_dataset(enricher_ds)

    resolver.begin()
    store = get_store(enricher_ds, resolver)
    store.sync(clear=True)
    internals = list(store.view(enricher_ds, external=False).entities())
    assert len(internals) == 3, internals
    externals = list(store.view(enricher_ds, external=True).entities())
    for external in externals:
        for statement in external.statements:
            assert not statement.external, statement

    view = store.view(enricher_ds)
    match = view.get_entity(canon_id)
    "Umbrella Corp." in match.get("name")
    "Umbrella Corporation" not in match.get("name")
    assert match.schema.name == "Company"
    _, ownership = list(view.get_inverted(canon_id))[0]
    owner = view.get_entity(ownership.get("owner")[0])
    assert owner.id == "osv-oswell-spencer"

    resolver.rollback()
    store.close()
    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_enrich_id_match(vcontext: Context):
    """We match an entity with same ID"""
    crawl_dataset(vcontext.dataset)
    enricher = load_enricher(vcontext, DATASET_DATA, "testdataset1")
    entity = Entity.from_data(vcontext.dataset, JON_DOVER)
    candidates = {id_.id: cands for id_, cands in enricher.candidates([entity])}

    # Not a match with a different ID
    assert entity.id != "osv-john-doe"
    assert not candidates, candidates
    assert len(list(enricher.match_candidates(entity, []))) == 0

    # But when the id matches, it's a match
    entity.id = "osv-john-doe"
    results = list(enricher.match_candidates(entity, []))
    assert len(results) == 1, results
    assert str(results[0].id) == entity.id, results[0]

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_expand_securities(vcontext: Context, testdataset_securities: Dataset):
    """test that we don't expand to sibling securities via the issuer"""
    crawl_dataset(testdataset_securities)
    enricher = load_enricher(vcontext, DATASET_DATA, testdataset_securities.name)
    entity = Entity.from_data(vcontext.dataset, AAA_USD_ISK)
    candidates = {id_.id: cands for id_, cands in enricher.candidates([entity])}

    # Match
    results = list(enricher.match_candidates(entity, candidates[entity.id]))
    assert len(results) == 2, results  # USD EUR is also a match
    assert str(results[0].id) == "osv-isin-a", results[0]

    # Expand
    internals = list(enricher.expand(results[0]))
    assert len(internals) == 2, internals

    assert internals[0].schema.name == "Security"
    assert internals[0].id == "osv-isin-a"
    assert internals[1].schema.name == "Organization"
    assert internals[1].id == "osv-lei-a"

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_expand_issuers(vcontext: Context, testdataset_securities: Dataset):
    """test that we expand to the securities of an issuer"""
    crawl_dataset(testdataset_securities)
    enricher = load_enricher(vcontext, DATASET_DATA, "testdataset_securities")
    entity = Entity.from_data(vcontext.dataset, AAA_BANK)
    candidates = {id_.id: cands for id_, cands in enricher.candidates([entity])}

    # Match
    results = list(enricher.match_candidates(entity, candidates[entity.id]))
    assert len(results) == 1, results
    assert str(results[0].id) == "osv-lei-a", results[0]

    # Expand
    internals = list(enricher.expand(results[0]))
    assert len(internals) == 3, internals

    assert internals[0].schema.name == "Organization"
    assert internals[0].id == "osv-lei-a"
    assert internals[1].schema.name == "Security"
    assert internals[2].schema.name == "Security"
    assert internals[1].id != internals[2].id
    assert internals[1].id[:-1] == internals[2].id[:-1] == "osv-isin-"

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_cutoff(vcontext: Context):
    """We don't match an entity if its score is lower than the cutoff."""
    crawl_dataset(vcontext.dataset)
    dataset_data = deepcopy(DATASET_DATA)
    dataset_data["config"]["cutoff"] = 0.99
    enricher = load_enricher(vcontext, dataset_data, "testdataset1")
    entity = Entity.from_data(vcontext.dataset, UMBRELLA_CORP)
    candidates = {id_.id: cands for id_, cands in enricher.candidates([entity])}
    results = list(enricher.match_candidates(entity, candidates[entity.id]))
    assert len(results) == 0, results

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_limit(vcontext: Context):
    """We only return limit matches per entity"""
    crawl_dataset(vcontext.dataset)
    dataset_data = deepcopy(DATASET_DATA)
    dataset_data["config"]["limit"] = 0
    enricher = load_enricher(vcontext, dataset_data, "testdataset1")
    entity = Entity.from_data(vcontext.dataset, UMBRELLA_CORP)
    candidates = {id_.id: cands for id_, cands in enricher.candidates([entity])}
    results = list(enricher.match_candidates(entity, candidates[entity.id]))
    assert len(results) == 0, results

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_is_edge_internal(testdataset1: Dataset):
    """is_edge_internal returns True iff every edge endpoint has a risk topic."""
    linker = get_dataset_linker(testdataset1)
    crawl_dataset(testdataset1)
    store = get_store(testdataset1, linker)
    store.sync()
    view = store.view(testdataset1, external=False)

    # Ownership: oswell-spencer (crime.boss) → umbrella-corp (reg.warn) — both have topics
    ownership = Entity.from_data(
        testdataset1,
        {
            "schema": "Ownership",
            "id": "test-ownership",
            "properties": {
                "owner": ["osv-oswell-spencer"],
                "asset": ["osv-umbrella-corp"],
            },
        },
    )
    assert is_edge_internal(ownership, view)

    # Family: johnny-does (no topics) → oswell-spencer (crime.boss) — one endpoint missing topic
    family_no_topic = Entity.from_data(
        testdataset1,
        {
            "schema": "Family",
            "id": "test-family-no-topic",
            "properties": {
                "person": ["osv-johnny-does"],
                "relative": ["osv-oswell-spencer"],
            },
        },
    )
    assert not is_edge_internal(family_no_topic, view)

    # Family: one endpoint not in view at all
    family_missing = Entity.from_data(
        testdataset1,
        {
            "schema": "Family",
            "id": "test-family-missing",
            "properties": {
                "person": ["osv-john-doe"],
                "relative": ["nonexistent-id"],
            },
        },
    )
    assert not is_edge_internal(family_missing, view)

    store.close()
    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_enrich_topic_gated(testdataset1: Dataset, testdataset_enrich_subject: Dataset):
    """With topic_gated=True, expanded entities without topics are emitted external."""
    resolver = get_resolver()
    crawl_dataset(testdataset_enrich_subject)
    crawl_dataset(testdataset1)

    dataset_data = deepcopy(DATASET_DATA)
    dataset_data["config"]["topic_gated"] = True
    enricher_ds = make_enricher_dataset(dataset_data, testdataset1.name)
    crawl_dataset(enricher_ds)

    # Confirm the match (decide requires an active transaction)
    resolver.begin()
    canon_id = resolver.decide("osv-umbrella-corp", "xxx", Judgement.POSITIVE)
    resolver.commit()

    clear_data_path(enricher_ds.name)
    crawl_dataset(enricher_ds)

    resolver.begin()
    store = get_store(enricher_ds, resolver)
    store.sync(clear=True)

    # The subject entity (Umbrella Corp.) has no topics in the subject store,
    # so all expanded entities should be emitted as external.
    internals = list(store.view(enricher_ds, external=False).entities())
    assert len(internals) == 0, internals

    all_entities = list(store.view(enricher_ds, external=True).entities())
    assert len(all_entities) > 0, all_entities

    # Canonical entity exists in the combined view
    canon_entity = store.view(enricher_ds, external=True).get_entity(canon_id)
    assert canon_entity is not None

    resolver.rollback()
    store.close()
    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)
