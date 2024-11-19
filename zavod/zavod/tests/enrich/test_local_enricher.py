from copy import deepcopy
from nomenklatura.enrich import make_enricher
from nomenklatura.entity import CompositeEntity
import shutil

from zavod import settings
from zavod.context import Context
from zavod.crawl import crawl_dataset
from zavod.meta import Dataset
from zavod.runner.local_enricher import LocalEnricher

PATH = "zavod.runner.local_enricher:LocalEnricher"
DATASET_DATA = {
    "name": "some_registry",
    "title": "Some Company Registry",
    "config": {"cutoff": 0.5},
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


def load_enricher(context: Context, dataset_data, target_dataset: str):
    dataset_data_copy = deepcopy(dataset_data)
    dataset_data_copy["config"]["dataset"] = target_dataset
    dataset_data_copy["config"]["type"] = PATH
    dataset = Dataset.make(dataset_data_copy)
    return make_enricher(dataset, context.cache, dataset.config)


def test_enrich(vcontext: Context):
    """We match and expand an entity with a similar name"""
    crawl_dataset(vcontext.dataset)
    enricher = load_enricher(vcontext, DATASET_DATA, "testdataset1")
    assert isinstance(enricher, LocalEnricher)
    entity = CompositeEntity.from_data(vcontext.dataset, UMBRELLA_CORP)

    # Match
    enricher.load(entity)
    candidates = {id_.id: cands for id_, cands in enricher.candidates()}
    results = list(enricher.match_candidates(entity, candidates[entity.id]))
    assert len(results) == 1, results
    assert str(results[0].id) == "osv-umbrella-corp", results[0]

    # Expand
    internals = list(enricher.expand(entity, results[0]))
    assert len(internals) == 3, internals

    assert internals[0].schema.name == "Company"
    assert internals[0].id == "osv-umbrella-corp"
    assert internals[1].schema.name == "Ownership"
    assert internals[1].get("owner") == ["osv-oswell-spencer"]
    assert internals[1].get("asset") == ["osv-umbrella-corp"]
    assert internals[2].schema.name == "Person"
    assert internals[2].id == "osv-oswell-spencer"

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
