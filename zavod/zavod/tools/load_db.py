from nomenklatura.resolver import Linker
from nomenklatura.db import insert_statements

from zavod import settings
from zavod.db import get_engine
from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.stateful.model import statement_table
from zavod.tools.util import iter_output_statements

log = get_logger(__name__)


def load_dataset_to_db(
    scope: Dataset,
    linker: Linker[Entity],
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
    engine = get_engine()
    for dataset in scope.leaves:
        insert_statements(
            engine,
            statement_table,
            dataset.name,
            iter_output_statements(dataset, linker, external=external),
            batch_size=batch_size,
        )
