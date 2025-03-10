from nomenklatura import Resolver
from nomenklatura.judgement import Judgement
from nomenklatura.statement import CSV, read_path_statements

from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.crawl import crawl_dataset
from zavod.tools.dump_file import dump_dataset_to_file
from zavod.archive import iter_dataset_statements, dataset_state_path


def test_dump_file(testdataset1: Dataset, resolver: Resolver[Entity]):
    assert resolver.get_edge("osv-john-doe", "osv-johnny-does") is None
    crawl_dataset(testdataset1)

    stmts = list(iter_dataset_statements(testdataset1))
    assert len(stmts) > 0

    canonical = resolver.decide(
        "osv-john-doe",
        "osv-johnny-does",
        Judgement.POSITIVE,
        user="test",
    )
    out_path = dataset_state_path(testdataset1.name) / "dump.csv"
    assert not out_path.exists()
    testdataset1.resolve = True
    dump_dataset_to_file(testdataset1, resolver, out_path, format=CSV)
    assert out_path.exists()
    assert out_path.stat().st_size > 0

    file_stmts = list(read_path_statements(out_path, CSV))
    assert len(file_stmts) == len(stmts)
    canon_ids = [stmt.canonical_id for stmt in file_stmts]
    assert canonical.id in canon_ids
