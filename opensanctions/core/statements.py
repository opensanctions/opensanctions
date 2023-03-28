from functools import cache
from datetime import datetime
from typing import Dict, Generator, List, Optional, Tuple, Union
from sqlalchemy.future import select
from sqlalchemy.sql.expression import delete, update, insert
from sqlalchemy.sql.functions import func
from zavod.logs import get_logger
from followthemoney import model
from followthemoney.types import registry
from nomenklatura.resolver import Resolver
from nomenklatura.statement import Statement

from opensanctions.core.db import stmt_table, canonical_table
from opensanctions.core.db import Conn, ConnCache, upsert_func
from opensanctions.core.dataset import Dataset

log = get_logger(__name__)


def save_statements(conn: Conn, statements: List[Statement]) -> None:
    # unique = {s["id"]: s for s in values}
    # values = list(unique.values())
    if not len(statements):
        return None
    log.debug("Saving statements", size=len(statements))
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
    conn: Conn, dataset=None, canonical_id=None, inverted_ids=None, external=False
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
    if external is False:
        q = q.filter(stmt_table.c.external == False)
    q = q.order_by(stmt_table.c.canonical_id.asc())
    result = conn.execute(q)
    for row in result.yield_per(20000):
        yield Statement.from_db_row(row)


def count_entities(
    conn: Conn,
    dataset: Optional[Dataset] = None,
    target: Optional[bool] = None,
    schemata: Optional[List[str]] = None,
) -> int:
    q = select(func.count(func.distinct(stmt_table.c.canonical_id)))
    q = q.filter(stmt_table.c.prop == Statement.BASE)
    q = q.filter(stmt_table.c.external == False)  # noqa
    if target is not None:
        q = q.filter(stmt_table.c.target == target)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    if schemata is not None and len(schemata):
        q = q.filter(stmt_table.c.schema.in_(schemata))
    return conn.scalar(q)


def agg_entities_by_country(
    conn: Conn,
    dataset: Optional[Dataset] = None,
    target: Optional[bool] = None,
    schemata: Optional[List[str]] = None,
) -> List[Dict[str, Union[str, int]]]:
    """Return the number of targets grouped by country."""
    count = func.count(func.distinct(stmt_table.c.canonical_id))
    q = select(stmt_table.c.value, count)
    # TODO: this could be generic to type values?
    q = q.filter(stmt_table.c.external == False)  # noqa
    q = q.filter(stmt_table.c.prop_type == registry.country.name)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    if target is not None:
        q = q.filter(stmt_table.c.target == target)
    if schemata is not None and len(schemata):
        q = q.filter(stmt_table.c.schema.in_(schemata))
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


def agg_entities_by_schema(
    conn: Conn,
    dataset: Optional[Dataset] = None,
    target: Optional[bool] = None,
    schemata: Optional[List[str]] = None,
) -> List[Dict[str, Union[str, int]]]:
    """Return the number of targets grouped by their schema."""
    # FIXME: duplicates entities when there are statements with different schema
    # defined for the same entity.
    count = func.count(func.distinct(stmt_table.c.canonical_id))
    q = select(stmt_table.c.schema, count)
    q = q.filter(stmt_table.c.external == False)  # noqa
    q = q.filter(stmt_table.c.prop == Statement.BASE)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    if target is not None:
        q = q.filter(stmt_table.c.target == target)
    if schemata is not None and len(schemata):
        q = q.filter(stmt_table.c.schema.in_(schemata))
    q = q.group_by(stmt_table.c.schema)
    q = q.order_by(count.desc())
    res = conn.execute(q)
    results = []
    for name, count in res.fetchall():
        schema = model.get(name)
        if schema is None or schema.hidden:
            continue
        result = {
            "name": name,
            "count": count,
            "label": schema.label,
            "plural": schema.plural,
        }
        results.append(result)
    return results


def all_schemata(conn: Conn, dataset: Optional[Dataset] = None) -> List[str]:
    """Return all schemata present in the dataset"""
    q = select(func.distinct(stmt_table.c.schema))
    q = q.filter(stmt_table.c.prop == Statement.BASE)
    q = q.filter(stmt_table.c.external == False)  # noqa
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    q = q.group_by(stmt_table.c.schema)
    res = conn.execute(q)
    return [s for (s,) in res.all()]


@cache
def _last_seen_by_dataset(conncache: ConnCache) -> Dict[str, datetime]:
    q = select(
        stmt_table.c.dataset.label("dataset"),
        func.max(stmt_table.c.last_seen).label("last_seen"),
    )
    q = q.filter(stmt_table.c.prop == Statement.BASE)
    q = q.filter(stmt_table.c.external == False)  # noqa
    q = q.group_by(stmt_table.c.dataset)
    cursor = conncache.conn.execute(q)
    return {r._mapping["dataset"]: r._mapping["last_seen"] for r in cursor}


def max_last_seen(conn: Conn, dataset: Optional[Dataset] = None) -> Optional[datetime]:
    """Return the latest date of the data."""
    last_seens = _last_seen_by_dataset(ConnCache(conn))
    if dataset is None:
        times = list(last_seens.values())
    else:
        times = [last_seens[d] for d in dataset.source_names if d in last_seens]
    return max(times, default=None)
    # q = select(func.max(stmt_table.c.last_seen))
    # q = q.filter(stmt_table.c.prop == Statement.BASE)
    # q = q.filter(stmt_table.c.external == False)  # noqa
    # if dataset is not None:
    #     q = q.filter(stmt_table.c.dataset.in_(dataset.source_names))
    # return conn.scalar(q)


def entities_datasets(
    conn: Conn, dataset: Optional[Dataset] = None
) -> Generator[Tuple[str, str], None, None]:
    """Return all entity IDs with the dataset they belong to."""
    q = select(stmt_table.c.entity_id, stmt_table.c.dataset)
    q = q.filter(stmt_table.c.prop == Statement.BASE)
    if dataset is not None:
        q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    q = q.distinct()
    result = conn.execution_options(stream_results=True).execute(q)
    for row in result:
        entity_id, scope = row
        yield (entity_id, scope)


def cleanup_dataset(conn: Conn, dataset: Dataset):
    # remove non-current statements (in the future we may want to keep them?)
    q = select(func.max(stmt_table.c.last_seen))
    q = q.filter(stmt_table.c.prop == Statement.BASE)
    q = q.filter(stmt_table.c.external == False)  # noqa
    q = q.filter(stmt_table.c.dataset == dataset.name)
    q = q.group_by(stmt_table.c.dataset)
    cursor = conn.execute(q)
    last_seen = cursor.scalar()
    if last_seen is not None:
        pq = delete(stmt_table)
        pq = pq.where(stmt_table.c.dataset == dataset.name)
        pq = pq.where(stmt_table.c.last_seen < last_seen)
        conn.execute(pq)


def lock_dataset(conn: Conn, dataset: Dataset):
    q = select(stmt_table.c.id)
    q = q.with_for_update()
    q = q.filter(stmt_table.c.dataset == dataset.name)
    conn.execute(q)


def resolve_all_canonical_via_table(conn: Conn, resolver: Resolver):
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


def resolve_all_canonical(conn: Conn, resolver: Resolver):
    log.info("Getting all canonical value pairs...")
    q = select(stmt_table.c.entity_id, stmt_table.c.canonical_id)
    q = q.distinct()
    # updated = 0
    pairs = conn.execute(q).fetchall()
    for (entity_id, current_id) in pairs:
        canonical_id = resolver.get_canonical(entity_id)
        if canonical_id != current_id:
            log.info("Resolve: %s -> %s", entity_id, canonical_id)
            # print("MISMATCH", entity_id, current_id, canonical_id)
            q = update(stmt_table)
            q = q.where(stmt_table.c.entity_id == entity_id)
            q = q.where(stmt_table.c.canonical_id != canonical_id)
            q = q.values({stmt_table.c.canonical_id: canonical_id})
            # print(q)
            conn.execute(q)
            # updated += 1

        # if updated > 0 and updated % 100 == 0:
        #     # conn.commit()
        #     pass


def resolve_canonical(conn: Conn, resolver: Resolver, canonical_id: str):
    referents = resolver.get_referents(canonical_id)
    log.debug("Resolving: %s" % canonical_id, referents=referents)
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
