from pathlib import Path
from followthemoney.statement.serialize import get_statement_writer
from nomenklatura.resolver import Linker

from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.runtime.manifest import Manifest
from zavod.tools.util import iter_output_statements, unique_statements

log = get_logger(__name__)


def dump_dataset_to_file(
    manifest: Manifest,
    linker: Linker[Entity],
    out_path: Path,
    format: str,
    external: bool = True,
) -> None:
    """Dump all the statements in the given scope to a file in one of the
    formats supported by nomenklatura.

    Args:
        manifest: The manifest pinning the dataset versions to dump.
        linker: The resolver linker applied to the statements.
        out_path: The database URI to load into.
        format: Format name defined by nomenklatura
        external: Include statements that are enrichment candidates.
    """
    with open(out_path, "wb") as fh:
        writer = get_statement_writer(fh, format)
        total_count: int = 0
        for dataset_name in manifest.datasets.keys():
            output = iter_output_statements(
                dataset_name, manifest, linker, external=external
            )
            stmts = unique_statements(output)
            for idx, stmt in enumerate(stmts):
                total_count += 1
                writer.write(stmt)
                if total_count % 10000 == 0:
                    log.info(
                        "Writing statements to file",
                        path=out_path.as_posix(),
                        dataset=dataset_name,
                        statements=idx + 1,
                        total=total_count,
                    )
        log.info(
            "Export complete",
            scope=manifest.scope.name,
            total=total_count,
        )
        writer.close()
