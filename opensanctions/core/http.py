import structlog
from typing import Optional
from datetime import timedelta
from sqlalchemy.future import select
from sqlalchemy.sql.expression import delete

from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.db import Conn, upsert_func, cache_table

log = structlog.get_logger("http")


async def save_cache(
    conn: Conn, url: str, dataset: Dataset, text: Optional[str]
) -> None:
    cache = {
        "timestamp": settings.RUN_TIME,
        "url": url,
        "dataset": dataset.name,
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


async def clear_cache(conn: Conn, dataset: Dataset):
    pq = delete(cache_table)
    pq = pq.where(cache_table.c.dataset == dataset.name)
    await conn.execute(pq)
