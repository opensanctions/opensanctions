import pytest
from nomenklatura.judgement import Judgement

from zavod.meta import Dataset
from zavod.store import get_store, clear_store
from zavod.crawl import crawl_dataset
from zavod.dedupe import get_resolver, blocking_xref, AUTO_USER
from zavod.dedupe import merge_entities, explode_cluster


def test_store_access(testdataset1: Dataset):
    crawl_dataset(testdataset1)
    resolver = get_resolver()
    assert not len(resolver.edges)

    store = get_store(testdataset1, external=True)
    blocking_xref(store)
    assert len(resolver.edges)
    for edge in resolver.edges.values():
        assert edge.score is not None
        assert edge.score >= 0.0
        assert edge.judgement == Judgement.NO_JUDGEMENT
        assert edge.user == AUTO_USER
    clear_store(testdataset1)


def test_resolve_dedupe(testdataset1: Dataset):
    stats = crawl_dataset(testdataset1)
    resolver = get_resolver()
    assert len(resolver.edges) == 0
    resolver.decide("osv-john-doe", "osv-johnny-does", Judgement.POSITIVE, user="test")
    store = get_store(testdataset1)
    view = store.default_view()
    for ent in view.entities():
        assert ent.id != "osv-john-doe"
        assert ent.id != "osv-johnny-does"
    ent_count = len(list(view.entities()))
    store.close()
    assert ent_count == stats.entities - 1
    assert len(resolver.edges) == 2
    get_resolver.cache_clear()
    clear_store(testdataset1)


def test_resolver_tools():
    resolver = get_resolver()
    assert resolver.get_canonical("foo") == "foo"
    assert resolver.get_canonical("bar") == "bar"
    new_id = merge_entities(["foo", "bar"])
    assert new_id != "foo"
    assert resolver.get_canonical("foo") == new_id

    resolver.decide("foo", "qux", Judgement.NEGATIVE)
    assert resolver.get_canonical("qux") != new_id
    qx_id = merge_entities(["qux", "quux"])

    assert resolver.get_canonical("qux") == qx_id
    explode_cluster(qx_id)
    assert resolver.get_canonical("qux") == "qux"
    assert resolver.get_judgement("foo", "qux") == Judgement.NO_JUDGEMENT

    resolver.decide("foo", "qux", Judgement.NEGATIVE)
    with pytest.raises(ValueError):
        merge_entities(["foo", "qux"])
    clus_id = merge_entities(["foo", "qux"], force=True)
    assert clus_id == new_id

    resolver.decide("foo", "quux", Judgement.UNSURE)
    clus_id = merge_entities(["foo", "quux"])
    assert clus_id == new_id
