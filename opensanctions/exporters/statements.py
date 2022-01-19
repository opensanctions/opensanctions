import csv
import structlog
from os import PathLike
from banal import as_bool

from opensanctions import settings
from opensanctions.core.db import engine_tx, engine_read
from opensanctions.core.statements import all_statements, clear_statements
from opensanctions.core.statements import save_statements, stmt_key
from opensanctions.util import iso_datetime

log = structlog.get_logger(__name__)
COLUMNS = [
    "id",
    "entity_id",
    "prop",
    "prop_type",
    "schema",
    "value",
    "dataset",
    "target",
    "unique",
    "first_seen",
    "last_seen",
    "canonical_id",
]


def export_statements():
    stmts_path = settings.DATASET_PATH.joinpath("statements.csv")
    log.info("Writing global statements list", path=stmts_path)
    export_statements_path(stmts_path)


def export_statements_path(path: PathLike):
    stmt_count = 0
    with open(path, "w", encoding=settings.ENCODING) as fh:
        with engine_read() as conn:
            writer = csv.writer(fh, dialect=csv.unix_dialect)
            writer.writerow(COLUMNS)
            buffer = []
            for stmt in all_statements(conn):
                row = [
                    stmt["id"],
                    stmt["entity_id"],
                    stmt["prop"],
                    stmt["prop_type"],
                    stmt["schema"],
                    stmt["value"],
                    stmt["dataset"],
                    stmt["target"],
                    stmt["unique"],
                    stmt["first_seen"].isoformat(),
                    stmt["last_seen"].isoformat(),
                    stmt["canonical_id"],
                ]
                buffer.append(row)
                if len(buffer) > 1000:
                    writer.writerows(buffer)
                    buffer = []
                stmt_count += 1

            if buffer:
                writer.writerows(buffer)
    log.info("Statement export complete", count=stmt_count)


def import_statements_path(path: PathLike):
    with engine_tx() as conn:
        clear_statements(conn)
        stmt_count = 0
        with open(path, "r", encoding=settings.ENCODING) as fh:
            reader = csv.DictReader(fh, dialect=csv.unix_dialect)
            buffer = []
            for row in reader:
                stmt_count += 1
                row["id"] = stmt_key(
                    row["dataset"],
                    row["entity_id"],
                    row["prop"],
                    row["value"],
                )
                row["target"] = as_bool(row["target"])
                row["unique"] = as_bool(row["unique"])
                row["last_seen"] = iso_datetime(row["last_seen"])
                row["first_seen"] = iso_datetime(row["first_seen"])
                # TODO do we want to do more validation?
                buffer.append(row)
                if len(buffer) >= 1000:
                    log.info("Import: %d..." % stmt_count)
                    save_statements(conn, buffer)
                    buffer = []

            if len(buffer):
                save_statements(conn, buffer)
    log.info("Statement import complete", count=stmt_count)
