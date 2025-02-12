import pytest
from nomenklatura import Resolver
from nomenklatura.judgement import Judgement

from zavod.archive import dataset_state_path
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.store import get_store
from zavod.crawl import crawl_dataset
from zavod.integration.dedupe import blocking_xref, AUTO_USER
from zavod.integration.dedupe import merge_entities, explode_cluster


def test_store_access(testdataset1: Dataset, resolver: Resolver[Entity]):
    crawl_dataset(testdataset1)
    assert not len(resolver.get_edges())

    store = get_store(testdataset1, resolver)
    store.sync()
    state_path = dataset_state_path(testdataset1.name)
    blocking_xref(resolver, store, state_path)
    assert len(resolver.get_edges()) > 0
    for edge in resolver.get_edges():
        assert edge.score is not None
        assert edge.score >= 0.0
        assert edge.judgement == Judgement.NO_JUDGEMENT
        assert edge.user == AUTO_USER


def test_resolve_dedupe(testdataset1: Dataset, resolver: Resolver[Entity]):
    stats = crawl_dataset(testdataset1)
    assert len(resolver.get_edges()) == 0
    resolver.decide("osv-john-doe", "osv-johnny-does", Judgement.POSITIVE, user="test")
    store = get_store(testdataset1, resolver)
    store.sync()
    view = store.default_view()
    for ent in view.entities():
        assert ent.id != "osv-john-doe"
        assert ent.id != "osv-johnny-does"
    ent_count = len(list(view.entities()))
    store.close()
    assert ent_count == stats.entities - 1
    assert len(resolver.get_edges()) == 2


def test_resolver_tools(resolver: Resolver[Entity]):
    assert resolver.get_canonical("foo") == "foo"
    assert resolver.get_canonical("bar") == "bar"
    new_id = merge_entities(resolver, ["foo", "bar"])
    assert new_id != "foo"
    assert resolver.get_canonical("foo") == new_id

    resolver.decide("foo", "qux", Judgement.NEGATIVE)
    assert resolver.get_canonical("qux") != new_id
    qx_id = merge_entities(resolver, ["qux", "quux"])

    assert resolver.get_canonical("qux") == qx_id
    explode_cluster(resolver, qx_id)
    assert resolver.get_canonical("qux") == "qux"
    assert resolver.get_judgement("foo", "qux") == Judgement.NO_JUDGEMENT

    resolver.decide("foo", "qux", Judgement.NEGATIVE)
    with pytest.raises(ValueError):
        merge_entities(resolver, ["foo", "qux"])
    clus_id = merge_entities(resolver, ["foo", "qux"], force=True)
    assert clus_id == new_id

    resolver.decide("foo", "quux", Judgement.UNSURE)
    clus_id = merge_entities(resolver, ["foo", "quux"])
    assert clus_id == new_id
