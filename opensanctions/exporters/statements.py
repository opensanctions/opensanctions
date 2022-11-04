from pathlib import Path
from typing import Generator, List
from zavod.logs import get_logger
from nomenklatura.statement import Statement
from nomenklatura.statement import read_path_statements, CSV
from nomenklatura.statement import write_statements

from opensanctions import settings
from opensanctions.core.db import engine_tx, engine_read
from opensanctions.core.statements import all_statements, clear_statements
from opensanctions.core.statements import save_statements

log = get_logger(__name__)


def dump_statements() -> Generator[Statement, None, None]:
    stmt_count = 0
    with engine_read() as conn:
        for stmt in all_statements(conn):
            yield stmt
            stmt_count += 1
    log.info("Statement export complete", count=stmt_count)


def export_statements():
    stmts_path = settings.DATASET_PATH.joinpath("statements.csv")
    export_statements_path(stmts_path)


def export_statements_path(path: Path):
    log.info("Writing global statements list", path=path)
    with open(path, "wb") as fh:
        write_statements(fh, CSV, dump_statements())


def import_statements_path(path: Path):
    with engine_tx() as conn:
        clear_statements(conn)
        stmt_count = 0
        buffer: List[Statement] = []
        for stmt in read_path_statements(path, CSV, Statement):
            # TODO do we want to do more validation?
            buffer.append(stmt)
            if len(buffer) >= 1000:
                log.info("Import: %d..." % stmt_count)
                save_statements(conn, buffer)
                buffer = []

        if len(buffer):
            save_statements(conn, buffer)
    log.info("Statement import complete", count=stmt_count)
