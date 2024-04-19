from nomenklatura.cache import Cache
from zavod.crawl import crawl_dataset
from zavod.meta import Dataset
from nomenklatura.enrich import get_enricher
from nomenklatura.enrich.common import Enricher
from nomenklatura.entity import CompositeEntity
from copy import deepcopy

PATH = "zavod.runner.local_enricher:LocalEnricher"
DATASET_DATA = {
    "name": "nominatim",
    "title": "Nomimatim",
    "config": {"dataset": "testdataset1", "threshold": 0.7},
}


def load_enricher(dataset_data):
    enricher_cls = get_enricher(PATH)
    assert issubclass(enricher_cls, Enricher)
    dataset = Dataset.make(dataset_data)
    cache = Cache.make_default(dataset)
    return enricher_cls(dataset, cache, dataset.config)


def make_entity(dataset):
    data = {
        "schema": "LegalEntity",
        "id": "xxx",
        "properties": {"name": ["Umbrella Corp."]},
    }
    ent = CompositeEntity.from_data(dataset, data)
    return ent


def test_enrich(testdataset1: Dataset):
    """"""
    crawl_dataset(testdataset1)
    enricher = load_enricher(DATASET_DATA)
    entity = make_entity(testdataset1)
    results = list(enricher.match(entity))
    assert len(results) == 1, results
    assert str(results[0].id) == "osv-umbrella-corp", results[0]

    adjacent = list(enricher.expand(entity, results[0]))
    assert len(adjacent) == 2, adjacent
    adjacent.remove(results[0])
    assert adjacent[0].schema.name == "Ownership"
    assert adjacent[0].get("owner") == ["osv-oswell-spencer"]
    assert adjacent[0].get("asset") == ["osv-umbrella-corp"]


def test_threshold(testdataset1: Dataset):
    """"""
    crawl_dataset(testdataset1)
    dataset_data = deepcopy(DATASET_DATA)
    dataset_data["config"]["threshold"] = 0.99
    enricher = load_enricher(dataset_data)
    entity = make_entity(testdataset1)
    results = list(enricher.match(entity))
    assert len(results) == 0, results
