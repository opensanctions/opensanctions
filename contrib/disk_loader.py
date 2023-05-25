import orjson
import plyvel
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
from opensanctions.core.db import Conn, engine_read
from opensanctions.core.resolver import get_resolver
from opensanctions.core.dataset import Dataset

log = get_logger(__name__)
db = plyvel.DB("/Users/pudo/tmp/stmtdb/", create_if_missing=True)


def all_statements(
    conn: Conn, dataset=None, canonical_id=None, inverted_ids=None, external=False
) -> Generator[Statement, None, None]:
    q = select(stmt_table)
    # if canonical_id is not None:
    #     q = q.filter(stmt_table.c.canonical_id == canonical_id)
    # if inverted_ids is not None:
    #     alias = stmt_table.alias()
    #     sq = select(func.distinct(alias.c.canonical_id))
    #     sq = sq.filter(alias.c.prop_type == registry.entity.name)
    #     sq = sq.filter(alias.c.value.in_(inverted_ids))
    #     q = q.filter(stmt_table.c.canonical_id.in_(sq))
    # if dataset is not None:
    #     q = q.filter(stmt_table.c.dataset.in_(dataset.scope_names))
    # if external is False:
    #     q = q.filter(stmt_table.c.external == False)
    # q = q.order_by(stmt_table.c.canonical_id.asc())
    # if canonical_id is None and inverted_ids is None:
    conn = conn.execution_options(stream_results=True)
    cursor = conn.execute(q)
    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for row in rows:
            yield Statement.from_db_row(row)
    # for row in result.yield_per(20000):
    #     yield Statement.from_db_row(row)


def read_all():
    dataset = Dataset.require("all")
    resolver = get_resolver()
    with engine_read() as conn:
        wb = db.write_batch()
        for idx, stmt in enumerate(all_statements(conn, dataset=dataset)):
            if idx > 0 and idx % 100000 == 0:
                log.info("Read %s statements", idx)
                wb.write()
                wb = db.write_batch()
            data = orjson.dumps(stmt.to_dict())
            key = f"e.{stmt.canonical_id}".encode("utf-8")
            wb.put(key, stmt.schema.encode("utf-8"))
            key = f"s.{stmt.canonical_id}.{stmt.id}".encode("utf-8")
            wb.put(key, data)
            if stmt.prop_type == registry.entity.name:
                vc = resolver.get_canonical(stmt.value)
                key = f"i.{vc}.{stmt.prop}.{stmt.canonical_id}".encode("utf-8")
                wb.put(key, stmt.canonical_id.encode("utf-8"))
        wb.write()


def load_one(id):
    len_ = 0
    for (k, v) in db.iterator(prefix=bytes(f"s.{id}.", "utf-8")):
        print(k, v)
        len_ += 1

    print(len_)
    # stmt = Statement.from_dict(orjson.loads(v))
    # print(stmt)


if __name__ == "__main__":
    read_all()
    # load_one("Q7747")
