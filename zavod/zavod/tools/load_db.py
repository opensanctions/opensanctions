from nomenklatura.resolver import Linker
from nomenklatura.db import insert_statements
from nomenklatura.settings import STATEMENT_BATCH

from zavod.db import get_engine
from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.stateful.model import statement_table
from zavod.runtime.manifest import Manifest
from zavod.tools.util import iter_output_statements

log = get_logger(__name__)


def load_dataset_to_db(
    manifest: Manifest,
    linker: Linker[Entity],
    batch_size: int = STATEMENT_BATCH,
    external: bool = True,
) -> None:
    """Load a dataset into a database given as a URI. This will delete all
    statements related to a dataset before inserting the current statements.

    Args:
        manifest: The manifest pinning the dataset versions to load.
        linker: The resolver linker applied to the statements.
        batch_size: The number of statements to insert in a single batch.
        external: Include statements that are enrichment candidates.
    """
    engine = get_engine()
    for dataset_name in manifest.datasets.keys():
        # Duplicate statement IDs are left to the upsert in insert_statements,
        # which keeps the first row of a conflict just like an in-process
        # dedupe would - without buffering every ID that has been seen.
        insert_statements(
            engine,
            statement_table,
            dataset_name,
            iter_output_statements(dataset_name, manifest, linker, external=external),
            batch_size=batch_size,
        )
