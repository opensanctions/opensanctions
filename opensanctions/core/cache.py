import math
import urllib3
from random import randint
from typing import cast, Optional, Generator, TypedDict, Dict
from datetime import datetime, timedelta
from sqlalchemy.future import select
from sqlalchemy.sql.expression import delete

from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.db import Conn, upsert_func, cache_table, engine_read

urllib3.disable_warnings()


class Cache(TypedDict):
    url: Optional[str]
    dataset: Optional[str]
    text: str
    timestamp: datetime


WARMED: Dict[str, Cache] = {}


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


def randomize_cache(days: int) -> timedelta:
    min_cache = max(1, math.ceil(days * 0.7))
    max_cache = math.ceil(days * 1.3)
    return timedelta(days=randint(min_cache, max_cache))


def check_cache(conn: Conn, url: str, cache_days: int) -> Optional[str]:
    max_age = randomize_cache(cache_days)
    cache_cutoff = settings.RUN_TIME - max_age

    cache = WARMED.get(url)
    if cache is None:
        return None
    if cache["timestamp"] < cache_cutoff:
        return None
    text = cache.get("text")
    if text is not None:
        return text

    q = select(cache_table.c.text)
    q = q.filter(cache_table.c.url == url)
    q = q.filter(cache_table.c.timestamp > cache_cutoff)
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


def warm_cache(like: str):
    with engine_read() as conn:
        for cache in all_cached(conn, like):
            url = cache.pop("url", None)
            if url is not None:
                cache.pop("dataset", None)
                WARMED[url] = cache


def clear_cache(conn: Conn, dataset: Dataset):
    pq = delete(cache_table)
    pq = pq.where(cache_table.c.dataset == dataset.name)
    conn.execute(pq)
