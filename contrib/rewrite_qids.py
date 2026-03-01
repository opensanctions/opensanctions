#!/usr/bin/env python
from functools import cache

import click
import requests
from typing import List

from rigour.ids.wikidata import is_qid
from nomenklatura import settings as nk_settings
from zavod.logs import configure_logging, get_logger
from zavod.db import get_engine, meta
from sqlalchemy import delete, update, or_

log = get_logger(__name__)
WD_API = "https://www.wikidata.org/w/api.php"
DATASETS = {"wd_peps", "wd_categories", "wikidata"}


@cache
def map_qid(qid: str) -> str:
    """Map a QID to a new value"""
    # query the wikidata API to get the new value for the QID
    params = {
        "action": "wbgetentities",
        "ids": qid,
        "format": "json",
    }
    headers = {"User-Agent": "Zavod QID Rewriter/1.0"}
    response = requests.get(WD_API, params=params, headers=headers)
    response.raise_for_status()
    data = response.json()
    if "entities" in data and qid in data["entities"]:
        entity = data["entities"][qid]
        target = entity.get("redirects", {}).get("to")
        if target is not None:
            return target
    return qid


def rewrite_qids(qids: List[str]):
    """Process the provided QIDs."""
    nk_settings.DB_STMT_TIMEOUT = 84600 * 1000
    engine = get_engine()
    meta.reflect(bind=engine)
    resolver_table = meta.tables.get("resolver")
    if resolver_table is None:
        log.error("Resolver table not found in the database.")
        return
    cache_table = meta.tables.get("cache")
    if cache_table is None:
        log.error("Cache table not found in the database.")
        return

    with engine.connect() as conn:
        # Update source and target columns in resolver table for each QID
        for qid in qids:
            new_qid = map_qid(qid)
            if new_qid == qid:
                log.info(f"No mapping found for {qid}, skipping.")
                continue

            log.info(f"Rewriting {qid} -> {new_qid}")

            # Update source column
            stmt_source = (
                update(resolver_table)
                .where(resolver_table.c.source == qid)
                .values(source=new_qid)
            )
            result_source = conn.execute(stmt_source)
            log.info(f"Updated {result_source.rowcount} rows in resolver.source")

            # Update target column
            stmt_target = (
                update(resolver_table)
                .where(resolver_table.c.target == qid)
                .values(target=new_qid)
            )
            result_target = conn.execute(stmt_target)
            log.info(f"Updated {result_target.rowcount} rows in resolver.target")

        conn.commit()

    # Build the DELETE query with filters
    # Filter 1: dataset column is one of the DATASETS
    # Filter 2: text column contains any of the qids (using LIKE)
    good_qids = [q for q in qids if is_qid(q) and len(q) > 4]
    like_conditions = [cache_table.c.text.like(f"%{qid}%") for qid in good_qids]
    stmt = delete(cache_table).where(
        cache_table.c.dataset.in_(DATASETS),
        or_(*like_conditions),
    )

    with engine.connect() as conn:
        result = conn.execute(stmt)
        conn.commit()
        log.info(f"Deleted {result.rowcount} rows from cache table")


@click.command()
@click.argument("qids", nargs=-1, required=True)
def main(qids):
    """Rewrite QIDs. Accepts one or more QID arguments to process."""
    configure_logging()
    rewrite_qids(list(qids))


if __name__ == "__main__":
    main()
