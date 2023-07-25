from nomenklatura.judgement import Judgement

from zavod.meta import Dataset
from zavod.store import get_store
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
