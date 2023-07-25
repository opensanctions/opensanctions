from sqlalchemy import select
from sqlalchemy import MetaData, create_engine
from sqlalchemy.pool import NullPool
from nomenklatura.statement.db import make_statement_table

from zavod.meta import Dataset
from zavod.runner import run_dataset
from zavod.tools.load_db import load_dataset_to_db
from zavod.archive import iter_dataset_statements, dataset_state_path


def test_load_db(vdataset: Dataset):
    run_dataset(vdataset)

    stmts = list(iter_dataset_statements(vdataset))
    assert len(stmts) > 0

    db_path = dataset_state_path(vdataset.name) / "dump.sqlite3"
    assert not db_path.exists()
    db_uri = "sqlite:///%s" % db_path.as_posix()
    batch_size = (len(stmts) // 2) - 1
    load_dataset_to_db(vdataset, db_uri, batch_size=batch_size)
    assert db_path.exists()
    assert db_path.stat().st_size > 0

    engine = create_engine(db_uri, poolclass=NullPool)
    metadata = MetaData()
    table = make_statement_table(metadata)
    with engine.connect() as conn:
        results = conn.execute(select(table.c.id)).fetchall()
        ids = [r.id for r in results]
        assert len(ids) == len(stmts)
