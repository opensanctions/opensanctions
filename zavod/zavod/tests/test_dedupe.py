from nomenklatura.judgement import Judgement

from zavod.meta import Dataset
from zavod.store import get_store, clear_store
from zavod.runner import run_dataset
from zavod.dedupe import get_resolver, blocking_xref, AUTO_USER


def test_store_access(vdataset: Dataset):
    run_dataset(vdataset)
    resolver = get_resolver()
    assert not len(resolver.edges)

    store = get_store(vdataset, external=True)
    blocking_xref(store)
    assert len(resolver.edges)
    for edge in resolver.edges.values():
        assert edge.score is not None
        assert edge.score >= 0.1
        assert edge.judgement == Judgement.NO_JUDGEMENT
        assert edge.user == AUTO_USER
    clear_store(vdataset)


def test_resolve_dedupe(vdataset: Dataset):
    stats = run_dataset(vdataset)
    resolver = get_resolver()
    assert len(resolver.edges) == 0
    resolver.decide("osv-john-doe", "osv-johnny-does", Judgement.POSITIVE, user="test")
    store = get_store(vdataset)
    view = store.default_view()
    for ent in view.entities():
        assert ent.id != "osv-john-doe"
        assert ent.id != "osv-johnny-does"
    ent_count = len(list(view.entities()))
    store.close()
    assert ent_count == stats.entities - 1
    assert len(resolver.edges) == 2
    get_resolver.cache_clear()
    clear_store(vdataset)
