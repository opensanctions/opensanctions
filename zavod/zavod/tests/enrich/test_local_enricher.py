from copy import deepcopy
from nomenklatura.enrich import get_enricher
from nomenklatura.enrich.common import Enricher
from nomenklatura.entity import CompositeEntity
import shutil

from zavod import settings
from zavod.context import Context
from zavod.crawl import crawl_dataset
from zavod.meta import Dataset

PATH = "zavod.runner.local_enricher:LocalEnricher"
DATASET_DATA = {
    "name": "some_registry",
    "title": "Some Company Registry",
    "config": {"dataset": "testdataset1", "cutoff": 0.5},
}


def load_enricher(context: Context, dataset_data):
    enricher_cls = get_enricher(PATH)
    assert issubclass(enricher_cls, Enricher)
    dataset = Dataset.make(dataset_data)
    return enricher_cls(dataset, context.cache, dataset.config)


UMBRELLA_CORP = {
    "schema": "LegalEntity",
    "id": "xxx",
    "properties": {"name": ["Umbrella Corp."]},
}


JON_DOVER = {
    "schema": "Person",
    "id": "abc-jona-dova",  # Initially different from dataset
    "properties": {
        "name": ["Jonathan Dover"]  # Different from dataset
    },
}


def test_enrich(vcontext: Context):
    """We match and expand an entity with a similar name"""
    crawl_dataset(vcontext.dataset)
    enricher = load_enricher(vcontext, DATASET_DATA)
    entity = CompositeEntity.from_data(vcontext.dataset, UMBRELLA_CORP)

    # Match
    results = list(enricher.match(entity))
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
    enricher = load_enricher(vcontext, DATASET_DATA)
    entity = CompositeEntity.from_data(vcontext.dataset, JON_DOVER)

    # Not a match with a different ID
    assert entity.id != "osv-john-doe"
    assert len(list(enricher.match(entity))) == 0

    # But when the id matches, it's a match
    entity.id = "osv-john-doe"
    results = list(enricher.match(entity))
    assert len(results) == 1, results
    assert str(results[0].id) == entity.id, results[0]

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_cutoff(vcontext: Context):
    """We don't match an entity if its score is lower than the cutoff."""
    crawl_dataset(vcontext.dataset)
    dataset_data = deepcopy(DATASET_DATA)
    dataset_data["config"]["cutoff"] = 0.99
    enricher = load_enricher(vcontext, dataset_data)
    entity = CompositeEntity.from_data(vcontext.dataset, UMBRELLA_CORP)
    results = list(enricher.match(entity))
    assert len(results) == 0, results

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)


def test_limit(vcontext: Context):
    """We only return limit matches per entity"""
    crawl_dataset(vcontext.dataset)
    dataset_data = deepcopy(DATASET_DATA)
    dataset_data["config"]["limit"] = 0
    enricher = load_enricher(vcontext, dataset_data)
    entity = CompositeEntity.from_data(vcontext.dataset, UMBRELLA_CORP)
    results = list(enricher.match(entity))
    assert len(results) == 0, results

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)
