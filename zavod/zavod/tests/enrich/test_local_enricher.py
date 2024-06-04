import shutil
from nomenklatura.cache import Cache
from zavod import settings
from zavod.context import Context
from zavod.crawl import crawl_dataset
from zavod.meta import Dataset
from nomenklatura.enrich import get_enricher
from nomenklatura.enrich.common import Enricher
from nomenklatura.entity import CompositeEntity
from copy import deepcopy

PATH = "zavod.runner.local_enricher:LocalEnricher"
DATASET_DATA = {
    "name": "some_registry",
    "title": "Some Company Registry",
    "config": {"dataset": "testdataset1", "threshold": 0.7},
}


def load_enricher(context: Context, dataset_data):
    enricher_cls = get_enricher(PATH)
    assert issubclass(enricher_cls, Enricher)
    dataset = Dataset.make(dataset_data)
    return enricher_cls(dataset, context.cache, dataset.config)


def make_entity(dataset):
    data = {
        "schema": "LegalEntity",
        "id": "xxx",
        "properties": {"name": ["Umbrella Corp."]},
    }
    ent = CompositeEntity.from_data(dataset, data)
    return ent


def test_enrich(vcontext: Context):
    """We match and expand an entity with a similar name"""
    crawl_dataset(vcontext.dataset)
    enricher = load_enricher(vcontext, DATASET_DATA)
    entity = make_entity(vcontext.dataset)
    results = list(enricher.match(entity))
    assert len(results) == 1, results
    assert str(results[0].id) == "osv-umbrella-corp", results[0]

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


def test_threshold(vcontext: Context):
    """We don't match an entity if its score is lower than the threshold."""
    crawl_dataset(vcontext.dataset)
    dataset_data = deepcopy(DATASET_DATA)
    dataset_data["config"]["threshold"] = 0.99
    enricher = load_enricher(vcontext, dataset_data)
    entity = make_entity(vcontext.dataset)
    results = list(enricher.match(entity))
    assert len(results) == 0, results

    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)
