"""Parquet statement artifacts ("the lake") for dataset runs.

After a crawl, the emitted ``statements.pack`` is converted into a typed
``statements.parquet`` in the run's artifact directory, satisfying the
statement relation contract in `nomenklatura.duck`. The conversion joins the
last successful run's statements on statement id to compute ``first_seen``
between the two runs - the crawl itself stamps every statement with the run
time only.
"""

from pathlib import Path

import duckdb
from followthemoney.dataset import Version
from nomenklatura import duck

from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.archive import (
    STATEMENTS_FILE,
    STATEMENTS_PARQUET,
    backfill_artifact,
    dataset_artifact_path,
    get_last_successful_version,
)

log = get_logger(__name__)

# Fields can be up to PROP_VALUE_MAX (30 MB), far beyond duckdb's default
# maximum line size.
PACK_MAX_LINE = 33554432
# Render TIMESTAMP columns back to the string form used by Statement.
SEEN_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _sql_str(value: Path) -> str:
    return str(value).replace("'", "''")


def _read_pack_sql(path: Path) -> str:
    """A SELECT adapting a statements.pack file to the statement relation contract.

    The pack dialect (csv.unix_dialect) is pinned rather than sniffed: a large
    unquoted sample makes the sniffer detect "no quoting", which then breaks on
    the first quoted field further into the file."""
    return f"""
        SELECT
            id,
            entity_id,
            split_part(prop, ':', 1) AS "schema",
            split_part(prop, ':', 2) AS prop,
            value,
            dataset,
            lang,
            original_value,
            origin,
            coalesce(external = 't', false) AS external,
            CAST(first_seen AS TIMESTAMP) AS first_seen,
            CAST(last_seen AS TIMESTAMP) AS last_seen
        FROM read_csv('{_sql_str(path)}', header = true, all_varchar = true,
            delim = ',', quote = '"', escape = '"', max_line_size = {PACK_MAX_LINE})
    """


def _empty_statements_sql() -> str:
    """A SELECT producing zero rows with the statement relation contract types."""
    columns = ", ".join(
        f'CAST(NULL AS {type_}) AS "{column}"'
        for column, type_ in duck.STATEMENT_COLUMNS.items()
    )
    return f"SELECT {columns} WHERE false"


def _previous_statements_sql(dataset: Dataset) -> str | None:
    """A SELECT over the last successful run's statements, or None for the
    first run of a dataset. Prefers the parquet artifact, falling back to
    reading the pack file for runs that predate the lake."""
    version = get_last_successful_version(dataset.name)
    if version is None:
        return None
    path = backfill_artifact(dataset.name, version, STATEMENTS_PARQUET)
    if path is not None:
        return f"SELECT * FROM read_parquet('{_sql_str(path)}')"
    path = backfill_artifact(dataset.name, version, STATEMENTS_FILE)
    if path is None:
        msg = f"Statements for {dataset.name}@{version.id} not found in the archive"
        raise FileNotFoundError(msg)
    if path.stat().st_size == 0:
        return None
    return _read_pack_sql(path)


def _check_pack_rows(conn: duckdb.DuckDBPyConnection, pack_sql: str) -> None:
    """Reject pack contents that cannot satisfy the statement contract."""
    row = conn.execute(
        f"""
        SELECT count(*) FILTER (WHERE "schema" = '' OR prop = '' OR id IS NULL)
        FROM ({pack_sql})
        """
    ).fetchone()
    assert row is not None
    if int(row[0]) > 0:
        raise ValueError(f"{row[0]} rows have a malformed prop or a null id")


def build_statements_parquet(dataset: Dataset, version: Version) -> None:
    """Build the run's statements.parquet from its emitted statements.pack.

    Runs after a crawl. The output satisfies the statement relation contract:
    deduplicated on statement id and sorted by entity_id, with ``first_seen``
    carried over from the last successful run by joining that run's statements
    on statement id - a statement absent from the previous run is first seen at
    this run's time. Previous-run external statements do not donate their
    ``first_seen``: a statement promoted from enrichment candidate to internal
    counts as new."""
    pack_path = dataset_artifact_path(dataset.name, version, STATEMENTS_FILE)
    if not pack_path.is_file():
        raise FileNotFoundError(f"No statements file: {pack_path}")
    out_path = dataset_artifact_path(dataset.name, version, STATEMENTS_PARQUET)
    conn = duck.connect()
    try:
        if pack_path.stat().st_size == 0:
            # The crawl emitted nothing (see Context.finalize_statements).
            select = _empty_statements_sql()
        else:
            pack_sql = _read_pack_sql(pack_path)
            _check_pack_rows(conn, pack_sql)
            previous_sql = _previous_statements_sql(dataset)
            if previous_sql is None:
                previous_sql = _empty_statements_sql()
            # Rows can share an id with differing content: the statement id hash
            # covers dataset, entity_id, prop, value, lang and external, so only
            # schema, original_value, origin and the seen timestamps can differ
            # within an id. Prefer the richest row, then order for determinism.
            select = f"""
                WITH crawled AS (
                    SELECT * FROM ({pack_sql})
                    QUALIFY row_number() OVER (
                        PARTITION BY id
                        ORDER BY original_value NULLS LAST, origin NULLS LAST,
                            "schema", first_seen, last_seen
                    ) = 1
                ),
                previous AS (
                    SELECT id, min(first_seen) AS first_seen
                    FROM ({previous_sql})
                    WHERE NOT external
                    GROUP BY id
                )
                SELECT
                    crawled.id,
                    crawled.entity_id,
                    crawled."schema",
                    crawled.prop,
                    crawled.value,
                    crawled.dataset,
                    crawled.lang,
                    crawled.original_value,
                    crawled.origin,
                    crawled.external,
                    coalesce(previous.first_seen, crawled.last_seen) AS first_seen,
                    crawled.last_seen
                FROM crawled
                LEFT JOIN previous USING (id)
            """
        tmp_path = out_path.with_suffix(".parquet.tmp")
        try:
            conn.execute(
                f"""
                COPY (SELECT * FROM ({select}) ORDER BY entity_id)
                TO '{_sql_str(tmp_path)}' (FORMAT parquet, COMPRESSION zstd)
                """
            )
            tmp_path.replace(out_path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        conn.execute(
            f"CREATE VIEW lake_out AS SELECT * FROM read_parquet('{_sql_str(out_path)}')"
        )
        duck.validate_statement_relation(conn, "lake_out")
        row = conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE first_seen = last_seen)"
            " FROM lake_out"
        ).fetchone()
        assert row is not None
        log.info(
            "Built statements.parquet",
            dataset=dataset.name,
            version=version.id,
            statements=int(row[0]),
            new=int(row[1]),
        )
    finally:
        conn.close()


def dump_statements_pack(dataset: Dataset, version: Version) -> None:
    """Regenerate the run's statements.pack from its statements.parquet.

    The parquet build is where statements get deduplicated and their
    ``first_seen`` computed, so dumping it back gives pack consumers the
    deduplicated, correctly time-stamped view again - the crawl-emitted pack
    only carries run-time stamps."""
    parquet_path = dataset_artifact_path(dataset.name, version, STATEMENTS_PARQUET)
    if not parquet_path.is_file():
        raise FileNotFoundError(f"No statements parquet file: {parquet_path}")
    pack_path = dataset_artifact_path(dataset.name, version, STATEMENTS_FILE)
    tmp_path = pack_path.with_suffix(".pack.tmp")
    conn = duck.connect()
    try:
        conn.execute(
            f"""
            COPY (
                SELECT
                    entity_id,
                    "schema" || ':' || prop AS prop,
                    value,
                    dataset,
                    lang,
                    original_value,
                    origin,
                    CASE WHEN external THEN 't' END AS external,
                    strftime(first_seen, '{SEEN_FORMAT}') AS first_seen,
                    strftime(last_seen, '{SEEN_FORMAT}') AS last_seen,
                    id
                FROM read_parquet('{_sql_str(parquet_path)}')
                ORDER BY entity_id
            ) TO '{_sql_str(tmp_path)}' (FORMAT csv, HEADER true)
            """
        )
        tmp_path.replace(pack_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    finally:
        conn.close()
