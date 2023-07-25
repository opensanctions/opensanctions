from pathlib import Path
from nomenklatura.statement.serialize import get_statement_writer

from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.tools.util import iter_output_statements

log = get_logger(__name__)


def dump_dataset_to_file(scope: Dataset, out_path: Path, format: str) -> None:
    """Dump all the statements in the given scope to a file in one of the
    formats supported by nomenklatura.

    Args:
        scope: The dataset to load from the archive.
        out_path: The database URI to load into.
        format: Format name defined by nomenklatura
    """
    with open(out_path, "wb") as fh:
        writer = get_statement_writer(fh, format)
        total_count: int = 0
        for dataset in scope.leaves:
            for idx, stmt in enumerate(iter_output_statements(dataset)):
                total_count += 1
                writer.write(stmt)
                if total_count % 10000 == 0:
                    log.info(
                        "Writing statements to file",
                        path=out_path.as_posix(),
                        dataset=dataset.name,
                        statements=idx + 1,
                        total=total_count,
                    )
        log.info(
            "Export complete",
            scope=scope.name,
            total=total_count,
        )
        writer.close()
