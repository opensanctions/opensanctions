from sqlalchemy import MetaData, create_engine
from sqlalchemy.pool import NullPool
from nomenklatura.statement.db import make_statement_table, insert_dataset

from zavod import settings
from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.tools.util import iter_output_statements

log = get_logger(__name__)


def load_dataset_to_db(
    scope: Dataset,
    database_uri: str,
    batch_size: int = settings.DB_BATCH_SIZE,
    external: bool = True,
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
    for dataset in scope.leaves:
        insert_dataset(
            engine,
            table,
            dataset.name,
            iter_output_statements(dataset, external=external),
            batch_size=batch_size,
        )
