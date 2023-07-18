from pathlib import Path
from typing import Generator, List, Set, Optional
from nomenklatura.statement import Statement
from nomenklatura.statement import read_path_statements, CSV
from nomenklatura.statement import write_statements

from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.dedupe import get_resolver
from zavod.archive import iter_dataset_statements, datasets_path
from opensanctions.core.db import engine_tx
from opensanctions.core.statements import clear_statements
from opensanctions.core.statements import save_statements

log = get_logger(__name__)


def dump_statements(
    dataset: Dataset, external: bool = False
) -> Generator[Statement, None, None]:
    stmt_count = 0
    resolver = get_resolver()
    seen: Set[str] = set()
    prev_dataset: Optional[str] = None
    for idx, stmt in enumerate(iter_dataset_statements(dataset, external=external)):
        if idx != 0 and idx % 50000 == 0:
            log.info("Exporting statements...", count=idx)
        if stmt.id in seen:
            continue
        if stmt.dataset != prev_dataset:
            prev_dataset = stmt.dataset
            seen = set()
        stmt.canonical_id = resolver.get_canonical(stmt.entity_id)
        yield stmt
        stmt_count += 1
        seen.add(stmt.id)
    log.info("Statement export complete", count=stmt_count)


def export_statements():
    stmts_path = datasets_path().joinpath("statements.csv")
    export_statements_path(stmts_path)


def export_statements_path(path: Path, dataset: Dataset, external: bool = False):
    log.info("Writing global statements list", path=path)
    with open(path, "wb") as fh:
        write_statements(fh, CSV, dump_statements(dataset, external=external))


def import_statements_path(path: Path):
    with engine_tx() as conn:
        clear_statements(conn)
        stmt_count = 0
        buffer: List[Statement] = []
        for stmt in read_path_statements(path, CSV, Statement):
            # TODO do we want to do more validation?
            buffer.append(stmt)
            if len(buffer) >= 5000:
                log.info("Import: %d..." % stmt_count)
                save_statements(conn, buffer)
                buffer = []

        if len(buffer):
            save_statements(conn, buffer)
    log.info("Statement import complete", count=stmt_count)
