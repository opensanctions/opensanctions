from followthemoney.statement import CSV, read_path_statements
from nomenklatura import Resolver
from nomenklatura.judgement import Judgement

from zavod import settings
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.crawl import crawl_dataset
from zavod.tools.dump_file import dump_dataset_to_file
from zavod.archive import dataset_state_path
from zavod.tests.util import get_manifest


def test_dump_file(testdataset1: Dataset, resolver: Resolver[Entity]):
    assert resolver.get_edge("osv-john-doe", "osv-johnny-does") is None
    crawl_dataset(testdataset1, settings.RUN_VERSION)
    manifest = get_manifest(testdataset1)

    stmts = list(manifest.statements())
    assert len(stmts) > 0

    canonical = resolver.decide(
        "osv-john-doe",
        "osv-johnny-does",
        Judgement.POSITIVE,
        user="test",
    )
    out_path = dataset_state_path(testdataset1.name) / "dump.csv"
    assert not out_path.exists()
    testdataset1.model.resolve = True
    dump_dataset_to_file(manifest, resolver, out_path, format=CSV)
    assert out_path.exists()
    assert out_path.stat().st_size > 0

    file_stmts = list(read_path_statements(out_path, CSV))
    assert len(file_stmts) == len(stmts)
    canon_ids = [stmt.canonical_id for stmt in file_stmts]
    assert canonical.id in canon_ids
