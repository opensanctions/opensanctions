import aiofiles
import structlog

from opensanctions import settings
from opensanctions.core.db import with_conn
from opensanctions.core.statements import all_statements
from opensanctions.exporters.common import write_object

log = structlog.get_logger(__name__)


async def export_statements():
    stmts_path = settings.DATASET_PATH.joinpath("statements.json")
    log.info("Writing global statements list", path=stmts_path)
    stmt_count = 0
    async with aiofiles.open(stmts_path, "w", encoding=settings.ENCODING) as fh:
        async with with_conn() as conn:
            async for stmt in all_statements(conn):
                await write_object(fh, stmt)
                stmt_count += 1
    log.info("Statement export complete", count=stmt_count)
