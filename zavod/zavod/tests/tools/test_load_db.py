from sqlalchemy import select
from nomenklatura.db import make_statement_table

from zavod.db import meta, get_engine
from zavod.meta import Dataset
from zavod.crawl import crawl_dataset
from zavod.tools.load_db import load_dataset_to_db
from zavod.integration.dedupe import get_dataset_linker
from zavod.archive import iter_dataset_statements


def test_load_db(testdataset1: Dataset):
    crawl_dataset(testdataset1)
    linker = get_dataset_linker(testdataset1)

    stmts = list(iter_dataset_statements(testdataset1))
    assert len(stmts) > 0

    batch_size = (len(stmts) // 2) - 1
    load_dataset_to_db(testdataset1, linker, batch_size=batch_size)

    engine = get_engine()
    table = make_statement_table(meta)
    with engine.connect() as conn:
        results = conn.execute(select(table.c.id)).fetchall()
        ids = [r.id for r in results]
        assert len(ids) == len(stmts)
