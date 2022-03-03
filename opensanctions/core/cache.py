import math
import urllib3
from random import randint
from typing import cast, Optional, Generator, TypedDict
from datetime import datetime, timedelta
from sqlalchemy.future import select
from sqlalchemy.sql.expression import delete

from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.db import Conn, upsert_func, cache_table

urllib3.disable_warnings()


class Cache(TypedDict):
    url: str
    dataset: str
    text: str
    timestamp: datetime


def save_cache(conn: Conn, url: str, dataset: Dataset, text: Optional[str]) -> None:
    cache: Cache = {
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
    conn.execute(stmt)
    return None


def check_cache(conn: Conn, url: str, max_age: timedelta) -> Optional[str]:
    q = select(cache_table.c.text)
    q = q.filter(cache_table.c.url == url)
    q = q.filter(cache_table.c.timestamp > (settings.RUN_TIME - max_age))
    q = q.order_by(cache_table.c.timestamp.desc())
    q = q.limit(1)
    result = conn.execute(q)
    row = result.fetchone()
    if row is not None:
        return row.text
    return None


def all_cached(conn: Conn, like: str) -> Generator[Cache, None, None]:
    q = select(cache_table)
    q = q.filter(cache_table.c.url.like(like))
    # q = q.filter(cache_table.c.timestamp > (settings.RUN_TIME - max_age))
    result = conn.execution_options(stream_results=True).execute(q)
    for row in result:
        yield cast(Cache, row._asdict())


def clear_cache(conn: Conn, dataset: Dataset):
    pq = delete(cache_table)
    pq = pq.where(cache_table.c.dataset == dataset.name)
    conn.execute(pq)


def randomize_cache(days: int) -> timedelta:
    min_cache = max(1, math.ceil(days * 0.7))
    max_cache = math.ceil(days * 1.3)
    return timedelta(days=randint(min_cache, max_cache))
