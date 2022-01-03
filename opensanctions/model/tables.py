from datetime import datetime
from banal import is_mapping
from typing import Any, Dict, Optional, TypedDict, cast
from sqlalchemy.ext.asyncio.engine import AsyncConnection
from sqlalchemy.types import JSON
from sqlalchemy.future import select
from sqlalchemy import delete, func
from sqlalchemy import Table, Column, Integer, DateTime, Unicode
from sqlalchemy.dialects.sqlite import insert as insert_sqlite
from sqlalchemy.dialects.postgresql import insert as insert_postgresql

from opensanctions import settings
from opensanctions.model.base import KEY_LEN, VALUE_LEN, metadata

Conn = AsyncConnection


class DB(object):
    def __init__(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")


issue_table = Table(
    "issue",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("timestamp", DateTime, nullable=False),
    Column("level", Unicode(KEY_LEN), nullable=False),
    Column("module", Unicode(KEY_LEN)),
    Column("dataset", Unicode(KEY_LEN), index=True, nullable=False),
    Column("message", Unicode(VALUE_LEN)),
    Column("entity_id", Unicode(KEY_LEN), index=True),
    Column("entity_schema", Unicode(KEY_LEN)),
    Column("data", JSON, nullable=False),
)


class Issue(TypedDict):
    id: int
    timestamp: datetime
    level: str
    module: Optional[str]
    dataset: str
    message: Optional[str]
    entity_id: Optional[str]
    entity_schema: Optional[str]
    data: Dict[str, Any]


resource_table = Table(
    "resource",
    metadata,
    Column("path", Unicode(KEY_LEN), primary_key=True, nullable=False),
    Column("dataset", Unicode(KEY_LEN), primary_key=True, index=True, nullable=False),
    Column("checksum", Unicode(KEY_LEN), nullable=False),
    Column("timestamp", DateTime, nullable=False),
    Column("mime_type", Unicode(KEY_LEN), nullable=True),
    Column("size", Integer, nullable=True),
    Column("title", Unicode(VALUE_LEN), nullable=True),
)


class Resource(TypedDict):
    path: str
    dataset: str
    checksum: str
    timestamp: datetime
    mime_type: Optional[str]
    mime_type_label: Optional[str]
    title: Optional[str]
    size: int


def upsert_func(conn: Conn):
    if conn.engine.dialect.name == "postgresql":
        return insert_postgresql
    return insert_sqlite


async def save_issue(conn: Conn, event: Dict[str, Any]):
    data = dict(event)
    for key, value in data.items():
        if hasattr(value, "to_dict"):
            value = value.to_dict()
        if isinstance(value, set):
            value = list(value)
        data[key] = value

    data.pop("_record", None)
    data.pop("timestamp", None)
    record = {
        "timestamp": settings.RUN_TIME,
        "module": data.pop("logger", None),
        "level": data.pop("level"),
        "message": data.pop("event", None),
        "dataset": data.pop("dataset"),
    }
    entity = data.pop("entity", None)
    if is_mapping(entity):
        record["entity_id"] = entity.get("id")
        record["entity_schema"] = entity.get("schema")
    elif isinstance(entity, str):
        record["entity_id"] = entity
    record["data"] = data
    q = issue_table.insert().values([record])
    await conn.execute(q)


async def save_resource(conn: Conn, path, dataset, checksum, mime_type, size, title):
    if size == 0:
        q = delete(resource_table)
        q = q.where(resource_table.c.dataset == dataset.name)
        q = q.where(resource_table.c.path == path)
        await conn.execute(q)
        return

    resource = {
        "dataset": dataset.name,
        "path": path,
        "mime_type": mime_type,
        "checksum": checksum,
        "timestamp": settings.RUN_TIME,
        "size": size,
        "title": title,
    }
    upsert = upsert_func()
    istmt = upsert(resource_table).values([resource])
    stmt = istmt.on_conflict_do_update(
        index_elements=["path", "dataset"],
        set_=dict(
            mime_type=istmt.excluded.mime_type,
            checksum=istmt.excluded.checksum,
            timestamp=istmt.excluded.timestamp,
            size=istmt.excluded.size,
            title=istmt.excluded.title,
        ),
    )
    await conn.execute(stmt)


async def all_issues(conn: Conn, dataset=None):
    q = select(issue_table)
    if dataset is not None:
        q = q.filter(issue_table.c.dataset.in_(dataset.source_names))
    q = q.order_by(issue_table.c.id.asc())
    result = await conn.stream(q)
    async for row in result:
        yield cast(Issue, row._asdict())


async def all_resources(conn: Conn, dataset=None):
    q = select(resource_table)
    if dataset is not None:
        q = q.filter(resource_table.c.dataset == dataset.name)
    q = q.order_by(resource_table.c.path.asc())
    result = await conn.stream(q)
    async for row in result:
        yield cast(Resource, row._asdict())


async def agg_issues_by_level(conn: Conn, dataset=None):
    q = select(issue_table.c.level, func.count(issue_table.c.id))
    if dataset is not None:
        q = q.filter(issue_table.c.dataset.in_(dataset.source_names))
    q = q.group_by(issue_table.c.level)
    res = await conn.execute(q)
    return {l: c for (l, c) in res.all()}


async def clear_issues(conn: Conn, dataset):
    pq = delete(issue_table)
    pq = pq.where(issue_table.c.dataset.in_(dataset.source_names))
    await conn.execute(pq)


async def clear_resources(conn: Conn, dataset):
    pq = delete(resource_table)
    pq = pq.where(resource_table.c.dataset == dataset.name)
    await conn.execute(pq)


import asyncio

from sqlalchemy.ext.asyncio import create_async_engine


async def async_main():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
        await conn.run_sync(metadata.create_all)

        q = issue_table.insert()
        q = q.values(
            {
                "timestamp": datetime.utcnow(),
                "level": "error",
                "dataset": "us_ofac_sdn",
                "message": "I am a banana",
                "data": {},
            }
        )
        await conn.execute(q)

    conn = await engine.connect()
    # select a Result, which will be delivered with buffered
    # results
    q = select(issue_table)
    result = await conn.execute(q)

    async for row in all_issues(conn):
        print(row)

    # for AsyncEngine created in function scope, close and
    # clean-up pooled connections
    await engine.dispose()


if __name__ == "__main__":
    # click async deco: https://github.com/pallets/click/issues/85#issuecomment-43378930
    asyncio.run(async_main())
