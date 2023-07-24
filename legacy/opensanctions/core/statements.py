from typing import Dict, Generator, List, Optional, Tuple
from sqlalchemy.future import select
from sqlalchemy.sql.expression import delete, update
from sqlalchemy.sql.functions import func
from nomenklatura.resolver import Resolver
from nomenklatura.statement import Statement

from zavod.logs import get_logger
from zavod.meta import Dataset
from opensanctions.core.db import stmt_table
from opensanctions.core.db import Conn, upsert_func

log = get_logger(__name__)


def save_statements(conn: Conn, statements: List[Statement]) -> None:
    # unique = {s["id"]: s for s in values}
    # values = list(unique.values())
    if not len(statements):
        return None
    # log.debug("Saving statements", size=len(statements))
    values = [s.to_dict() for s in statements]
    istmt = upsert_func(stmt_table).values(values)
    stmt = istmt.on_conflict_do_update(
        index_elements=["id"],
        set_=dict(
            canonical_id=istmt.excluded.canonical_id,
            schema=istmt.excluded.schema,
            prop_type=istmt.excluded.prop_type,
            target=istmt.excluded.target,
            lang=istmt.excluded.lang,
            original_value=istmt.excluded.original_value,
            last_seen=istmt.excluded.last_seen,
        ),
    )
    conn.execute(stmt)
    return None


def all_statements(
    conn: Conn, dataset: Dataset = None, external: bool = False
) -> Generator[Statement, None, None]:
    q = select(stmt_table)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.leaf_names))
    if external is False:
        q = q.filter(stmt_table.c.external == False)  # noqa
    # q = q.order_by(stmt_table.c.canonical_id.asc())
    conn = conn.execution_options(stream_results=True)
    cursor = conn.execute(q)
    while True:
        rows = cursor.fetchmany(20000)
        if not rows:
            break
        for row in rows:
            yield Statement.from_db_row(row)
    # for row in result.yield_per(20000):
    #     yield Statement.from_db_row(row)


def count_entities(
    conn: Conn,
    dataset: Optional[Dataset] = None,
    target: Optional[bool] = None,
    schemata: Optional[List[str]] = None,
) -> int:
    q = select(func.count(func.distinct(stmt_table.c.canonical_id)))
    q = q.filter(stmt_table.c.prop_type == Statement.BASE)
    q = q.filter(stmt_table.c.external == False)  # noqa
    if target is not None:
        q = q.filter(stmt_table.c.target == target)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    if schemata is not None and len(schemata):
        q = q.filter(stmt_table.c.schema.in_(schemata))
    return conn.scalar(q)


def lock_dataset(conn: Conn, dataset: Dataset):
    q = select(stmt_table.c.id)
    q = q.with_for_update()
    q = q.filter(stmt_table.c.dataset == dataset.name)
    conn.execute(q)


def entities_datasets(
    conn: Conn, dataset: Optional[Dataset] = None
) -> Generator[Tuple[str, str], None, None]:
    """Return all entity IDs with the dataset they belong to."""
    q = select(stmt_table.c.entity_id, stmt_table.c.dataset)
    q = q.filter(stmt_table.c.prop_type == Statement.BASE)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.leaf_names))
    q = q.distinct()
    result = conn.execution_options(stream_results=True).execute(q)
    for row in result:
        entity_id, scope = row
        yield (entity_id, scope)


def cleanup_dataset(conn: Conn, dataset: Dataset):
    # remove non-current statements (in the future we may want to keep them?)
    q = select(func.max(stmt_table.c.last_seen))
    q = q.filter(stmt_table.c.prop == Statement.BASE)
    # q = q.filter(stmt_table.c.external == False)  # noqa
    q = q.filter(stmt_table.c.dataset == dataset.name)
    q = q.group_by(stmt_table.c.dataset)
    cursor = conn.execute(q)
    last_seen = cursor.scalar()
    if last_seen is not None:
        pq = delete(stmt_table)
        pq = pq.where(stmt_table.c.dataset == dataset.name)
        pq = pq.where(stmt_table.c.last_seen < last_seen)
        conn.execute(pq)


def resolve_all_canonical(conn: Conn, resolver: Resolver):
    log.info("Getting all canonical value pairs...", resolver=resolver)
    q = select(stmt_table.c.entity_id, stmt_table.c.canonical_id)
    q = q.distinct()
    # updated = 0
    updates: Dict[str, str] = {}
    cursor = conn.execute(q)
    while True:
        pairs = cursor.fetchmany(5000)
        if not pairs:
            break
        for (entity_id, current_id) in pairs:
            canonical_id = resolver.get_canonical(entity_id)
            if canonical_id != current_id:
                log.info(
                    "Resolve",
                    entity_id=entity_id,
                    canonical_id=canonical_id,
                    current_id=current_id,
                )
                updates[entity_id] = canonical_id

    log.info("Collected updates", count=len(updates))
    for entity_id, canonical_id in updates.items():
        uq = update(stmt_table)
        uq = uq.where(stmt_table.c.entity_id == entity_id)
        uq = uq.where(stmt_table.c.canonical_id != canonical_id)
        uq = uq.values({stmt_table.c.canonical_id: canonical_id})
        conn.execute(uq)


def resolve_canonical(conn: Conn, resolver: Resolver, canonical_id: str):
    referents = resolver.get_referents(canonical_id)
    log.debug(
        "Resolving",
        canonical=canonical_id,
        referents=referents,
        resolver=resolver,
    )
    q = update(stmt_table)
    q = q.where(stmt_table.c.entity_id.in_(referents))
    q = q.values({stmt_table.c.canonical_id: canonical_id})
    conn.execute(q)


def clear_statements(
    conn: Conn,
    dataset: Optional[Dataset] = None,
    external: Optional[bool] = None,
):
    q = delete(stmt_table)
    if external is not None:
        q = q.filter(stmt_table.c.external == external)
    # TODO: should this do collections?
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset == dataset.name)
    conn.execute(q)
