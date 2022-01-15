import csv
import aiocsv
import aiofiles
import structlog
from os import PathLike
from banal import as_bool

from opensanctions import settings
from opensanctions.core.db import with_conn
from opensanctions.core.statements import all_statements, clear_statements
from opensanctions.core.statements import save_statements, stmt_key
from opensanctions.util import iso_datetime

log = structlog.get_logger(__name__)
COLUMNS = [
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


async def export_statements():
    stmts_path = settings.DATASET_PATH.joinpath("statements.csv")
    log.info("Writing global statements list", path=stmts_path)
    await export_statements_path(stmts_path)


async def export_statements_path(path: PathLike):
    stmt_count = 0
    async with aiofiles.open(path, "w", encoding=settings.ENCODING) as fh:
        async with with_conn() as conn:
            writer = aiocsv.AsyncWriter(fh, dialect=csv.unix_dialect)
            await writer.writerow(COLUMNS)
            buffer = []
            async for stmt in all_statements(conn):
                row = [
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
                    await writer.writerows(buffer)
                    buffer = []
                stmt_count += 1

            if buffer:
                await writer.writerows(buffer)
    log.info("Statement export complete", count=stmt_count)


async def import_statements_path(path: PathLike):
    async with with_conn() as conn:
        await clear_statements(conn)
        stmt_count = 0
        async with aiofiles.open(path, "r", encoding=settings.ENCODING) as fh:
            reader = aiocsv.AsyncDictReader(fh, dialect=csv.unix_dialect)
            buffer = []
            async for row in reader:
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
                    await save_statements(conn, buffer)
                    buffer = []

            if len(buffer):
                await save_statements(conn, buffer)
    log.info("Statement import complete", count=stmt_count)
