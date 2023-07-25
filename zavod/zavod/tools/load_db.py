from typing import List, Dict, Any, cast
from sqlalchemy import delete, insert
from sqlalchemy import MetaData, create_engine
from sqlalchemy.pool import NullPool
from nomenklatura.statement.db import make_statement_table
from nomenklatura.util import iso_datetime

from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.tools.util import iter_output_statements

log = get_logger(__name__)


def load_dataset_to_db(
    scope: Dataset, database_uri: str, batch_size: int = 5000, external: bool = True
) -> None:
    """Load a dataset into a database given as a URI. This will delete all
    statements related to a dataset before inserting the current statements.

    Args:
        scope: The dataset to load from the archive.
        database_uri: The database URI to load into.
        batch_size: The number of statements to insert in a single batch.
        external: Include statements that are enrichment candidates.
    """
    engine = create_engine(database_uri, poolclass=NullPool)
    metadata = MetaData()
    table = make_statement_table(metadata)
    metadata.create_all(bind=engine, tables=[table])
    total_count: int = 0
    for dataset in scope.leaves:
        with engine.begin() as conn:
            del_q = delete(table).where(table.c.dataset == dataset.name)
            conn.execute(del_q)
            batch: List[Dict[str, Any]] = []
            dataset_count: int = 0
            for stmt in iter_output_statements(dataset, external=external):
                # Convert the statement to a dictionary, and convert the
                # timestamps to fit into SQLite.
                row = cast(Dict[str, Any], stmt.to_dict())
                for key in ("first_seen", "last_seen"):
                    value = row.pop(key, None)
                    if value is not None:
                        row[key] = iso_datetime(value)
                batch.append(row)

                total_count += 1
                dataset_count += 1
                if len(batch) >= batch_size:
                    log.info(
                        "Inserting batch of %s statements" % len(batch),
                        dataset=dataset.name,
                        statements=dataset_count,
                        total=total_count,
                    )
                    conn.execute(insert(table).values(batch))
                    batch = []
            if len(batch):
                conn.execute(table.insert().values(batch))
            log.info(
                "Load complete",
                dataset=dataset.name,
                statements=dataset_count,
                total=total_count,
            )
    # engine.dispose()
