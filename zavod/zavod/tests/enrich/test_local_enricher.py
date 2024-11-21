from copy import deepcopy
from typing import Iterable, List
import shutil

from nomenklatura.enrich import make_enricher
from nomenklatura.entity import CompositeEntity
from nomenklatura.statement import Statement
from nomenklatura.judgement import Judgement

from zavod import settings
from zavod.archive import iter_dataset_statements
from zavod.context import Context
from zavod.crawl import crawl_dataset
from zavod.meta import Dataset
from zavod.runner.local_enricher import LocalEnricher
from zavod.dedupe import get_resolver
from zavod.store import get_store

DATASET_DATA = {
    "name": "test_enricher",
    "title": "An enrichment dataset",
    "config": {"cutoff": 0.5},
    "entry_point": "zavod.runner.local_enricher:enrich",
    "inputs": ["enrichment_subject"],
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
    dataset_data_copy["config"]["dataset"] = target_dataset
    return Dataset.make(dataset_data_copy)


def load_enricher(context: Context, dataset_data, target_dataset: str):
    dataset = make_enricher_dataset(dataset_data, target_dataset)
    return dataset, LocalEnricher(dataset, context.cache, dataset.config)


def get_statements(dataset: Dataset, prop: str, external: bool) -> List[Statement]:
    return [
        s
        for s in iter_dataset_statements(dataset, external=external)
        if s.prop == prop and s.external == external
    ]


def test_enrich(testdataset1: Dataset, enrichment_subject: Dataset):
    """We match and expand an entity with a similar name"""

    # Make a little subject dataset
    entity = CompositeEntity.from_data(enrichment_subject, UMBRELLA_CORP)
    subject_context = Context(enrichment_subject)
    subject_context.emit(entity)
    subject_context.close()

    # Treat testdataset1 as the target dataset
    crawl_dataset(testdataset1)

    # Enrich the subject against the target
    resolver = get_resolver()
    assert len(resolver.edges) == 0
    enricher_ds = make_enricher_dataset(DATASET_DATA, testdataset1.name)
    stats = crawl_dataset(enricher_ds)

    internals = get_statements(enricher_ds, "id", False)
    assert len(internals) == 0, internals
    externals = get_statements(enricher_ds, "id", True)
    assert len(externals) == 1, externals

    # Judge a match
    canon_id = resolver.decide("osv-umbrella-corp", "xxx", Judgement.POSITIVE)

    # Enrich again, now with internals
    crawl_dataset(enricher_ds)
    internals = get_statements(enricher_ds, "id", False)
    assert len(internals) == 3, internals
    externals = get_statements(enricher_ds, "id", True)
    assert len(externals) == 0, externals

    enrichment_store = get_store(enricher_ds, resolver)
    enrichment_store.sync()
    enrichment_view = enrichment_store.view(enricher_ds)
    match = enrichment_view.get_entity(canon_id)
    assert match.schema.name == "Company"
    _, ownership = list(enrichment_view.get_inverted(canon_id))[0]
    owner = enrichment_view.get_entity(ownership.get("owner")[0])
    assert owner.id == "osv-oswell-spencer"

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_enrich_id_match(vcontext: Context):
    """We match an entity with same ID"""
    crawl_dataset(vcontext.dataset)
    enricher = load_enricher(vcontext, DATASET_DATA, "testdataset1")
    entity = CompositeEntity.from_data(vcontext.dataset, JON_DOVER)
    enricher.load(entity)
    candidates = {id_.id: cands for id_, cands in enricher.candidates()}

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
    enricher = load_enricher(vcontext, DATASET_DATA, "testdataset_securities")
    entity = CompositeEntity.from_data(vcontext.dataset, AAA_USD_ISK)
    enricher.load(entity)
    candidates = {id_.id: cands for id_, cands in enricher.candidates()}

    # Match
    results = list(enricher.match_candidates(entity, candidates[entity.id]))
    assert len(results) == 2, results  # USD EUR is also a match
    assert str(results[0].id) == "osv-isin-a", results[0]

    # Expand
    internals = list(enricher.expand(entity, results[0]))
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
    entity = CompositeEntity.from_data(vcontext.dataset, AAA_BANK)
    enricher.load(entity)
    candidates = {id_.id: cands for id_, cands in enricher.candidates()}

    # Match
    results = list(enricher.match_candidates(entity, candidates[entity.id]))
    assert len(results) == 1, results
    assert str(results[0].id) == "osv-lei-a", results[0]

    # Expand
    internals = list(enricher.expand(entity, results[0]))
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
    entity = CompositeEntity.from_data(vcontext.dataset, UMBRELLA_CORP)
    enricher.load(entity)
    candidates = {id_.id: cands for id_, cands in enricher.candidates()}
    results = list(enricher.match_candidates(entity, candidates[entity.id]))
    assert len(results) == 0, results

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_limit(vcontext: Context):
    """We only return limit matches per entity"""
    crawl_dataset(vcontext.dataset)
    dataset_data = deepcopy(DATASET_DATA)
    dataset_data["config"]["limit"] = 0
    enricher = load_enricher(vcontext, dataset_data, "testdataset1")
    entity = CompositeEntity.from_data(vcontext.dataset, UMBRELLA_CORP)
    enricher.load(entity)
    candidates = {id_.id: cands for id_, cands in enricher.candidates()}
    results = list(enricher.match_candidates(entity, candidates[entity.id]))
    assert len(results) == 0, results

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)
