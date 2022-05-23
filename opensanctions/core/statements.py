import structlog
from hashlib import sha1
from datetime import datetime
from typing import Generator, List, Optional, TypedDict, cast
from sqlalchemy.future import select
from sqlalchemy.sql.expression import delete, update, insert
from sqlalchemy.sql.functions import func
from nomenklatura import Resolver
from followthemoney import model
from followthemoney.types import registry

from opensanctions import settings
from opensanctions.core.db import stmt_table, canonical_table
from opensanctions.core.db import Conn, upsert_func
from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity

log = structlog.get_logger(__name__)
BASE = "id"


class Statement(TypedDict):
    """A single statement about a property relevant to an entity.

    For example, this could be useddocker to say: "In dataset A, entity X has the
    property `name` set to 'John Smith'. I first observed this at K, and last
    saw it at L."

    Null property values are not supported. This might need to change if we
    want to support making property-less entities.
    """

    id: str
    entity_id: str
    canonical_id: str
    prop: str
    prop_type: str
    schema: str
    value: str
    dataset: str
    target: bool
    first_seen: datetime
    last_seen: datetime


def stmt_key(dataset, entity_id, prop, value):
    """Hash the key properties of a statement record to make a unique ID."""
    key = f"{dataset}.{entity_id}.{prop}.{value}"
    return sha1(key.encode("utf-8")).hexdigest()


def statements_from_entity(entity: Entity, dataset: Dataset) -> List[Statement]:
    if entity.id is None or entity.schema is None:
        return []
    values: List[Statement] = [
        {
            "id": stmt_key(dataset.name, entity.id, BASE, entity.id),
            "entity_id": entity.id,
            "canonical_id": entity.id,
            "prop": BASE,
            "prop_type": BASE,
            "schema": entity.schema.name,
            "value": entity.id,
            "dataset": dataset.name,
            "target": entity.target,
            "first_seen": settings.RUN_TIME,
            "last_seen": settings.RUN_TIME,
        }
    ]
    for prop, value in entity.itervalues():
        stmt: Statement = {
            "id": stmt_key(dataset.name, entity.id, prop.name, value),
            "entity_id": entity.id,
            "canonical_id": entity.id,
            "prop": prop.name,
            "prop_type": prop.type.name,
            "schema": entity.schema.name,
            "value": value,
            "dataset": dataset.name,
            "target": entity.target,
            "first_seen": settings.RUN_TIME,
            "last_seen": settings.RUN_TIME,
        }
        values.append(stmt)
    return values


def save_statements(conn: Conn, values: List[Statement]) -> None:
    # unique = {s["id"]: s for s in values}
    # values = list(unique.values())
    if not len(values):
        return None
    log.debug("Saving statements", size=len(values))

    istmt = upsert_func(stmt_table).values(values)
    stmt = istmt.on_conflict_do_update(
        index_elements=["id"],
        set_=dict(
            canonical_id=istmt.excluded.canonical_id,
            schema=istmt.excluded.schema,
            prop_type=istmt.excluded.prop_type,
            target=istmt.excluded.target,
            last_seen=istmt.excluded.last_seen,
        ),
    )
    conn.execute(stmt)
    return None


def all_statements(
    conn: Conn, dataset=None, canonical_id=None, inverted_ids=None
) -> Generator[Statement, None, None]:
    q = select(stmt_table)
    if canonical_id is not None:
        q = q.filter(stmt_table.c.canonical_id == canonical_id)
    if inverted_ids is not None:
        alias = stmt_table.alias()
        sq = select(func.distinct(alias.c.canonical_id))
        sq = sq.filter(alias.c.prop_type == registry.entity.name)
        sq = sq.filter(alias.c.value.in_(inverted_ids))
        q = q.filter(stmt_table.c.canonical_id.in_(sq))
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    q = q.order_by(stmt_table.c.canonical_id.asc())
    result = conn.execution_options(stream_results=True).execute(q)
    for row in result:
        yield cast(Statement, row._asdict())


def count_entities(
    conn: Conn,
    dataset: Optional[Dataset] = None,
    target: Optional[bool] = None,
) -> int:
    q = select(func.count(func.distinct(stmt_table.c.canonical_id)))
    q = q.filter(stmt_table.c.prop == BASE)
    if target is not None:
        q = q.filter(stmt_table.c.target == target)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    return conn.scalar(q)


def agg_targets_by_country(conn: Conn, dataset: Optional[Dataset] = None):
    """Return the number of targets grouped by country."""
    count = func.count(func.distinct(stmt_table.c.canonical_id))
    q = select(stmt_table.c.value, count)
    # TODO: this could be generic to type values?
    q = q.filter(stmt_table.c.target == True)  # noqa
    q = q.filter(stmt_table.c.prop_type == registry.country.name)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    q = q.group_by(stmt_table.c.value)
    q = q.order_by(count.desc())
    res = conn.execute(q)
    countries = []
    for code, count in res.fetchall():
        result = {
            "code": code,
            "count": count,
            "label": registry.country.caption(code),
        }
        countries.append(result)
    return countries


def agg_targets_by_schema(conn: Conn, dataset: Optional[Dataset] = None):
    """Return the number of targets grouped by their schema."""
    # FIXME: duplicates entities when there are statements with different schema
    # defined for the same entity.
    count = func.count(func.distinct(stmt_table.c.canonical_id))
    q = select(stmt_table.c.schema, count)
    q = q.filter(stmt_table.c.target == True)  # noqa
    q = q.filter(stmt_table.c.prop == BASE)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    q = q.group_by(stmt_table.c.schema)
    q = q.order_by(count.desc())
    res = conn.execute(q)
    schemata = []
    for name, count in res.fetchall():
        schema = model.get(name)
        if schema is None:
            continue
        result = {
            "name": name,
            "count": count,
            "label": schema.label,
            "plural": schema.plural,
        }
        schemata.append(result)
    return schemata


# def recent_targets(conn: Conn, dataset: Optional[Dataset] = None, limit: int = 100):
#     """Return the N most recently added entities."""
#     q = select(stmt_table)
#     q = q.filter(stmt_table.c.target == True)  # noqa
#     q = q.filter(stmt_table.c.prop == "createdAt")
#     if dataset is not None:
#         q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
#     q = q.order_by(stmt_table.c.value.desc())
#     q = q.limit(limit)
#     res = conn.execute(q)
#     targets = []
#     for row in res.fetchall():
#         result = {
#             "canonical_id": row.canonical_id,
#             "created_at": row.value,
#             "dataset": row.dataset,
#         }
#         targets.append(result)
#     return targets


def all_schemata(conn: Conn, dataset: Optional[Dataset] = None):
    """Return all schemata present in the dataset"""
    q = select(func.distinct(stmt_table.c.schema))
    q = q.filter(stmt_table.c.prop == BASE)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    q = q.group_by(stmt_table.c.schema)
    res = conn.execute(q)
    return [s for (s,) in res.all()]


def max_last_seen(conn: Conn, dataset: Optional[Dataset] = None) -> Optional[datetime]:
    """Return the latest date of the data."""
    q = select(func.max(stmt_table.c.last_seen))
    q = q.filter(stmt_table.c.prop == BASE)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    return conn.scalar(q)


def entities_datasets(conn: Conn, dataset: Optional[Dataset] = None):
    """Return all entity IDs with the dataset they belong to."""
    q = select(stmt_table.c.entity_id, stmt_table.c.dataset)
    q = q.filter(stmt_table.c.prop == BASE)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    q = q.distinct()
    result = conn.execution_options(stream_results=True).execute(q)
    for row in result:
        entity_id, dataset = row
        yield (entity_id, dataset)


def cleanup_dataset(conn: Conn, dataset: Dataset):
    # set the entity BASE to the earliest spotting of the entity:
    # table = stmt_table.c.__table__
    # cte = select(
    #     func.min(table.c.first_seen).label("first_seen"),
    #     table.c.entity_id.label("entity_id"),
    # )
    # cte = cte.where(table.c.dataset == dataset.name)
    # cte = cte.group_by(table.c.entity_id)
    # cte = cte.cte("seen")
    # sq = select(cte.c.first_seen)
    # sq = sq.where(cte.c.entity_id == table.c.entity_id)
    # sq = sq.limit(1)
    # q = update(table)
    # q = q.where(table.c.dataset == dataset.name)
    # q = q.where(table.c.prop == BASE)
    # q = q.values({table.c.first_seen: sq.scalar_subquery()})
    # # log.info("Setting BASE first_seen...", q=str(q))
    # db.session.execute(q)

    # remove non-current statements (in the future we may want to keep them?)
    last_seen = max_last_seen(conn, dataset=dataset)
    if last_seen is not None:
        pq = delete(stmt_table)
        pq = pq.where(stmt_table.c.dataset == dataset.name)
        pq = pq.where(stmt_table.c.last_seen < last_seen)
        conn.execute(pq)


def resolve_all_canonical(conn: Conn, resolver: Resolver):
    log.info("Resolving canonical_id in statements...", resolver=resolver)
    conn.execute(delete(canonical_table))
    mappings = []
    log.debug("Building canonical table mapping...")
    for canonical in resolver.canonicals():
        for referent in resolver.get_referents(canonical, canonicals=False):
            mappings.append({"entity_id": referent, "canonical_id": canonical.id})
        if len(mappings) > 5000:
            stmt = insert(canonical_table).values(mappings)
            conn.execute(stmt)
            mappings = []

    if len(mappings):
        stmt = insert(canonical_table).values(mappings)
        conn.execute(stmt)

    log.debug("Removing exploded canonical IDs...")
    q = update(stmt_table)
    q = q.where(stmt_table.c.canonical_id != stmt_table.c.entity_id)
    nested_q = select(canonical_table.c.entity_id)
    nested_q = nested_q.where(canonical_table.c.entity_id == stmt_table.c.entity_id)
    q = q.where(~nested_q.exists())
    q = q.values({stmt_table.c.canonical_id: stmt_table.c.entity_id})
    conn.execute(q)

    log.debug("Applying canonical IDs from canonical table to statements...")
    q = update(stmt_table)
    q = q.where(stmt_table.c.entity_id == canonical_table.c.entity_id)
    q = q.where(stmt_table.c.canonical_id != canonical_table.c.canonical_id)
    q = q.values({stmt_table.c.canonical_id: canonical_table.c.canonical_id})
    conn.execute(q)


def resolve_canonical(conn: Conn, resolver: Resolver, canonical_id: str):
    referents = resolver.get_referents(canonical_id)
    log.debug("Resolving: %s" % canonical_id, referents=referents)
    q = update(stmt_table)
    q = q.where(stmt_table.c.entity_id.in_(referents))
    q = q.values({stmt_table.c.canonical_id: canonical_id})
    conn.execute(q)


def clear_statements(conn: Conn, dataset: Optional[Dataset] = None):
    q = delete(stmt_table)
    # TODO: should this do collections?
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset == dataset.name)
    conn.execute(q)
