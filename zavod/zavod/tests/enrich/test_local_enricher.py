import shutil
from copy import deepcopy

import pytest
from nomenklatura.judgement import Judgement

from zavod import settings
from zavod.entity import Entity
from zavod.archive import clear_data_path, dataset_state_path
from zavod.context import Context
from nomenklatura.db import make_session
from zavod.crawl import crawl_dataset
from zavod.exc import RunFailedException
from zavod.integration.dedupe import get_resolver
from zavod.meta import Dataset
from zavod.runner.local_enricher import LocalEnricher
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
    "properties": {"name": ["Umbrella Corp."], "topics": ["reg.warn"]},
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

    # Make a little subject dataset
    crawl_dataset(testdataset_enrich_subject)

    # Treat testdataset1 as the target dataset
    crawl_dataset(testdataset1)

    # Enrich the subject against the target
    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        assert list(resolver.get_candidates()) == []
        assert list(resolver.get_judgements()) == []
    enricher_ds = make_enricher_dataset(DATASET_DATA, testdataset1.name)
    crawl_dataset(enricher_ds)
    assert enricher_ds.name == "test_enricher"

    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        store = get_store(enricher_ds, resolver)
        store.sync(clear=True)
        internals = list(store.view(enricher_ds, external=False).entities())
        assert len(internals) == 0, internals
        externals = list(store.view(enricher_ds, external=True).entities())
        assert len(externals) == 1, externals
        store.close()

        # Judge a match candidate (committed on block exit, before the next crawl)
        canon_id = resolver.decide("osv-umbrella-corp", "xxx", Judgement.POSITIVE)

    # Enrich again, now with internals
    clear_data_path(enricher_ds.name)
    crawl_dataset(enricher_ds)

    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
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


def test_topic_gated_requires_topics(testdataset1: Dataset):
    """topic_gated without `topics` is a config error: subjects wouldn't be
    topic-filtered, so untagged matches could emit disconnected supporting
    entities."""
    crawl_dataset(testdataset1)
    dataset_data = deepcopy(DATASET_DATA)
    dataset_data["config"]["topic_gated"] = True
    enricher_ds = make_enricher_dataset(dataset_data, testdataset1.name)
    with pytest.raises(RunFailedException):
        crawl_dataset(enricher_ds)
    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_enrich_topic_gated(testdataset1: Dataset, testdataset_enrich_subject: Dataset):
    """With topic_gated=True, the matched entity (which has a topic) is emitted
    internal; an adjacent untagged risk-target entity is emitted external. Once
    that neighbour is tagged in the subject store, a subsequent run promotes
    both the node and the connecting edge to internal."""
    clear_data_path(testdataset_enrich_subject.name)
    crawl_dataset(testdataset_enrich_subject)  # enriching this
    crawl_dataset(testdataset1)  # enriching against this

    # Confirm the match (committed on block exit, before the enrich crawl)
    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        canon_id = resolver.decide("osv-umbrella-corp", "xxx", Judgement.POSITIVE)

    dataset_data = deepcopy(DATASET_DATA)
    dataset_data["config"]["topic_gated"] = True
    dataset_data["config"]["topics"] = ["reg.warn", "crime.boss"]
    enricher_ds = make_enricher_dataset(dataset_data, testdataset1.name)
    clear_data_path(enricher_ds.name)
    crawl_dataset(enricher_ds)

    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        store = get_store(enricher_ds, resolver)
        store.sync(clear=True)
        view_internal = store.view(enricher_ds, external=False)
        view_all = store.view(enricher_ds, external=True)

        # The matched entity has topic "reg.warn" in the subject store → emitted internal.
        internals = list(view_internal.entities())
        assert len(internals) == 1, internals
        assert internals[0].id == canon_id

        # oswell-spencer is reachable from the match but absent from the subject store
        # (only the enrich subject is in scope), so it must be external only.
        assert view_all.get_entity("osv-oswell-spencer") is not None
        # oswell-spencer has no topic in the subject store → must be external
        assert view_internal.get_entity("osv-oswell-spencer") is None, ""

        # Ownership edge: oswell-spencer endpoint has no topic → external.
        inverted = list(view_all.get_inverted(canon_id))
        ownership_entities = [e for _, e in inverted if e.schema.edge]
        assert len(ownership_entities) == 1, ownership_entities
        ownership_id = ownership_entities[0].id
        # ownership edge has an untagged endpoint → must be external
        assert view_internal.get_entity(ownership_id) is None
        store.close()

    # --- Round 2: give oswell-spencer a risk topic in the subject store ---
    # Simulate what the graph analyzer would do after the first enricher run:
    # tag an adjacent (currently external) entity with a topic and emit external.
    subject_ctx = Context(testdataset_enrich_subject)
    subject_ctx.emit(Entity.from_data(testdataset_enrich_subject, UMBRELLA_CORP))
    oswell_patch = Entity.from_data(
        testdataset_enrich_subject,
        {
            "schema": "Person",
            "id": "osv-oswell-spencer",
            "properties": {"topics": ["crime.boss"]},
        },
    )
    subject_ctx.emit(oswell_patch, external=True)
    subject_ctx.close()

    shutil.rmtree(
        dataset_state_path(testdataset_enrich_subject.name) / "store",
        ignore_errors=True,
    )
    clear_data_path(enricher_ds.name)
    crawl_dataset(enricher_ds)

    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        store = get_store(enricher_ds, resolver)
        store.sync(clear=True)
        view_internal = store.view(enricher_ds, external=False)
        view_all = store.view(enricher_ds, external=True)

        # Both endpoints of the ownership now carry risk topics → all three emitted internal.
        internals = list(view_internal.entities())
        # umbrella-corp, oswell-spencer, ownership edge
        assert len(internals) == 3, internals

        assert view_internal.get_entity(canon_id) is not None
        # oswell-spencer now has a risk topic in the subject store → must be internal
        assert view_internal.get_entity("osv-oswell-spencer") is not None

        inverted = list(view_all.get_inverted(canon_id))
        ownership_entities = [e for _, e in inverted if e.schema.edge]
        assert len(ownership_entities) == 1, ownership_entities
        ownership_id = ownership_entities[0].id
        # ownership edge: both endpoints now have topics → must be internal
        assert view_internal.get_entity(ownership_id) is not None
        store.close()
    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)
