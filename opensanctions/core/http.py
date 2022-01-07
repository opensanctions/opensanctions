import structlog
from typing import Optional
from datetime import timedelta
from sqlalchemy.future import select

from opensanctions import settings
from opensanctions.core.db import Conn, with_conn, upsert_func, cache_table

log = structlog.get_logger("http")
HEADERS = {"User-Agent": settings.USER_AGENT}


async def save_cache(conn: Conn, url: str, text: Optional[str]) -> None:
    cache = {
        "timestamp": settings.RUN_TIME,
        "url": url,
        "text": text,
    }
    istmt = upsert_func(cache_table).values([cache])
    stmt = istmt.on_conflict_do_update(
        index_elements=["url"],
        set_=dict(
            timestamp=istmt.excluded.timestamp,
            text=istmt.excluded.text,
        ),
    )
    await conn.execute(stmt)
    return None


async def check_cache(conn: Conn, url: str, max_age: timedelta) -> Optional[str]:
    q = select(cache_table.c.text)
    q = q.filter(cache_table.c.url == url)
    q = q.filter(cache_table.c.timestamp > (settings.RUN_TIME - max_age))
    q = q.order_by(cache_table.c.timestamp.desc())
    q = q.limit(1)
    result = await conn.execute(q)
    row = result.fetchone()
    if row is not None:
        return row.text
    return None
