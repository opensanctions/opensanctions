import requests_mock
from nomenklatura.cache import Cache
from zavod.meta import Dataset
from nomenklatura.enrich import get_enricher, enrich, match
from nomenklatura.enrich.common import Enricher
from nomenklatura.entity import CompositeEntity
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver

PATH = "zavod.runner.local_enricher:LocalEnricher"
dataset = Dataset.make(
    {"name": "nominatim", "title": "Nomimatim", "config": {"dataset": "testdataset1"}}
)


def load_enricher():
    enricher_cls = get_enricher(PATH)
    assert issubclass(enricher_cls, Enricher)
    cache = Cache.make_default(dataset)
    return enricher_cls(dataset, cache, {})


def entity(testdataset1: Dataset):
    data = {
        "schema": "LegalEntity",
        "id": "xxx",
        "properties": {"name": ["Umbrella Corp."]},
    }
    ent = CompositeEntity.from_data(dataset, data)
    return ent


def test_match(testdataset1: Dataset):
    """"""
    enricher = load_enricher()
    results = list(enricher.match(entity()))
    assert len(results) == 1, results
    assert results[0].id == "osm-node-2140755199", results[0]

    adjacent = list(enricher.expand(entity(), results[0]))
    assert len(adjacent) == 1, adjacent
