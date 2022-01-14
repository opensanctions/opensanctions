from banal import is_mapping
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, Generator, Optional, TypedDict, cast
from sqlalchemy.future import select
from sqlalchemy.sql.expression import delete
from sqlalchemy.sql.functions import func

from opensanctions import settings
from opensanctions.core.db import issue_table, Conn
from opensanctions.core.dataset import Dataset


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


async def save_issue(conn: Conn, event: Dict[str, Any]) -> None:
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
    return None


async def all_issues(
    conn: Conn, dataset: Optional[Dataset] = None
) -> AsyncGenerator[Issue, None]:
    q = select(issue_table)
    if dataset is not None:
        q = q.filter(issue_table.c.dataset.in_(dataset.source_names))
    q = q.order_by(issue_table.c.id.asc())
    result = await conn.stream(q)
    async for row in result:
        yield cast(Issue, row._asdict())


async def agg_issues_by_level(
    conn: Conn, dataset: Optional[Dataset] = None
) -> Dict[str, int]:
    q = select(issue_table.c.level, func.count(issue_table.c.id))
    if dataset is not None:
        q = q.filter(issue_table.c.dataset.in_(dataset.source_names))
    q = q.group_by(issue_table.c.level)
    res = await conn.execute(q)
    return {l: c for (l, c) in res.all()}


async def clear_issues(conn: Conn, dataset: Dataset) -> None:
    pq = delete(issue_table)
    pq = pq.where(issue_table.c.dataset.in_(dataset.source_names))
    await conn.execute(pq)
    return None
