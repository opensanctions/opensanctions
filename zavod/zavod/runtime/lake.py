"""Parquet statement artifacts ("the lake") for dataset runs.

After a crawl, the emitted ``statements.raw`` is converted into a typed
``statements.parquet`` in the run's artifact directory, satisfying the
statement relation contract in `nomenklatura.duck`. The conversion joins the
last successful run's statements on statement id to compute ``first_seen``
between the two runs - the crawl itself stamps every statement with the run
time only. The published ``statements.pack`` is then dumped back out of the
parquet, so both carry the deduplicated, correctly time-stamped statements.
"""

from pathlib import Path

import duckdb
from followthemoney.dataset import Version
from nomenklatura import duck
from nomenklatura.duck import check_relation_name

from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.runtime.manifest import Manifest
from zavod.archive import (
    STATEMENTS_FILE,
    STATEMENTS_PARQUET,
    STATEMENTS_RAW,
    backfill_artifact,
    dataset_artifact_path,
    get_artifact_object,
    get_last_successful_version,
)

log = get_logger(__name__)

# Fields can be up to PROP_VALUE_MAX (30 MB), far beyond duckdb's default
# maximum line size.
PACK_MAX_LINE = 33554432
# Render TIMESTAMP columns back to the string form used by Statement.
SEEN_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _sql_str(value: Path | str) -> str:
    return str(value).replace("'", "''")


def _read_pack_sql(path: Path) -> str:
    """A SELECT adapting a pack-format statements file to the statement
    relation contract.

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


def _dedupe_sql(select: str) -> str:
    """Wrap a statement SELECT to keep one row per statement id.

    Rows can share an id with differing content: the statement id hash
    covers dataset, entity_id, prop, value, lang and external, so only
    schema, original_value, origin and the seen timestamps can differ
    within an id. Prefer the richest row, then order for determinism."""
    return f"""
        SELECT * FROM ({select})
        QUALIFY row_number() OVER (
            PARTITION BY id
            ORDER BY original_value NULLS LAST, origin NULLS LAST,
                "schema", first_seen, last_seen
        ) = 1
    """


def _empty_statements_sql() -> str:
    """A SELECT producing zero rows with the statement relation contract types."""
    columns = ", ".join(
        f'CAST(NULL AS {type_}) AS "{column}"'
        for column, type_ in duck.STATEMENT_COLUMNS.items()
    )
    return f"SELECT {columns} WHERE false"


def _version_statements_sql(
    conn: duckdb.DuckDBPyConnection, dataset_name: str, version: Version
) -> str | None:
    """A SELECT over one dataset version's statements, or None for a version
    with no statements (a pre-lake empty run).

    Prefers the parquet artifact: a local copy first, then read in place via
    the archive object's URI so that only the queried columns are fetched;
    when the URI cannot be read (old objects can be access-restricted) the
    file is backfilled and read locally instead. Versions that predate the
    lake fall back to a backfill of the pack file, which a remote read could
    not skip any bytes of anyway."""
    path = dataset_artifact_path(dataset_name, version, STATEMENTS_PARQUET)
    if path.is_file():
        return f"SELECT * FROM read_parquet('{_sql_str(path)}')"
    object = get_artifact_object(dataset_name, version, STATEMENTS_PARQUET)
    if object is not None:
        select = f"SELECT * FROM read_parquet('{_sql_str(object.uri())}')"
        try:
            conn.execute(f"DESCRIBE {select}")
            return select
        except duckdb.IOException as ioe:
            log.info(
                "Parquet not remotely readable, backfilling it",
                uri=object.uri(),
                error=str(ioe),
            )
        path_ = backfill_artifact(dataset_name, version, STATEMENTS_PARQUET)
        if path_ is not None:
            return f"SELECT * FROM read_parquet('{_sql_str(path_)}')"
    # The pack fallback is only for archived versions predating the lake: a
    # version that exists only locally comes from a crawl, which always
    # builds the parquet - a pre-existing local pack must not stand in.
    if get_artifact_object(dataset_name, version, STATEMENTS_FILE) is None:
        msg = f"No statement artifacts for {dataset_name}@{version.id}"
        raise FileNotFoundError(msg)
    pack = backfill_artifact(dataset_name, version, STATEMENTS_FILE)
    assert pack is not None
    if pack.stat().st_size == 0:
        return None
    # Convert the pack into a local parquet cache rather than reading the
    # CSV in place: a csv reader allocates buffers sized for the biggest
    # possible line, so a relation unioning dozens of pack reads runs out
    # of memory at bind time. Converting scans one file at a time, and
    # later consumers of the version get the parquet for free.
    log.info(
        "Converting archived pack to parquet",
        dataset=dataset_name,
        version=version.id,
    )
    tmp_path = path.with_suffix(".parquet.tmp")
    try:
        conn.execute(
            f"""
            COPY (SELECT * FROM ({_dedupe_sql(_read_pack_sql(pack))})
                ORDER BY entity_id)
            TO '{_sql_str(tmp_path)}' (FORMAT parquet, COMPRESSION zstd)
            """
        )
        tmp_path.replace(path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return f"SELECT * FROM read_parquet('{_sql_str(path)}')"


def _previous_statements_sql(
    conn: duckdb.DuckDBPyConnection, dataset: Dataset
) -> str | None:
    """A SELECT over the last successful run's statements, or None for the
    first run of a dataset."""
    version = get_last_successful_version(dataset.name)
    if version is None:
        return None
    return _version_statements_sql(conn, dataset.name, version)


def manifest_statements_view(
    conn: duckdb.DuckDBPyConnection,
    manifest: Manifest,
    relation: str = "lake_statements",
) -> str:
    """Create a view combining the statements of every dataset version pinned
    in the manifest, and return its name.

    This is how batch consumers (stores, exports, xref) get one statement
    relation for a whole scope out of the per-dataset artifacts. Each pinned
    version resolves through `_version_statements_sql`, so archived datasets
    are read as parquet where available and local, unpublished runs are read
    from their local artifact directory."""
    check_relation_name(relation)
    selects: list[str] = []
    for dataset_name, version in sorted(manifest.datasets.items()):
        select = _version_statements_sql(conn, dataset_name, version)
        if select is None:
            log.info(
                "Dataset version has no statements",
                dataset=dataset_name,
                version=version.id,
            )
            continue
        selects.append(select)
    if len(selects) == 0:
        selects.append(_empty_statements_sql())
    union = " UNION ALL ".join(f"SELECT * FROM ({select})" for select in selects)
    conn.execute(f"CREATE OR REPLACE VIEW {relation} AS {union}")
    duck.validate_statement_relation(conn, relation)
    return relation


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
    """Build the run's statements.parquet from its emitted statements.raw.

    Runs after a crawl. The output satisfies the statement relation contract:
    deduplicated on statement id and sorted by entity_id, with ``first_seen``
    carried over from the last successful run by joining that run's statements
    on statement id - a statement absent from the previous run is first seen at
    this run's time. Previous-run external statements do not donate their
    ``first_seen``: a statement promoted from enrichment candidate to internal
    counts as new."""
    raw_path = dataset_artifact_path(dataset.name, version, STATEMENTS_RAW)
    if not raw_path.is_file():
        raise FileNotFoundError(f"No raw statements file: {raw_path}")
    out_path = dataset_artifact_path(dataset.name, version, STATEMENTS_PARQUET)
    conn = duck.connect()
    try:
        if raw_path.stat().st_size == 0:
            # The crawl emitted nothing (see Context.finalize_statements).
            select = _empty_statements_sql()
        else:
            raw_sql = _read_pack_sql(raw_path)
            _check_pack_rows(conn, raw_sql)
            previous_sql = _previous_statements_sql(conn, dataset)
            if previous_sql is None:
                previous_sql = _empty_statements_sql()
            select = f"""
                WITH raw AS (
                    {_dedupe_sql(raw_sql)}
                ),
                previous AS (
                    SELECT id, min(first_seen) AS first_seen
                    FROM ({previous_sql})
                    WHERE NOT external
                    GROUP BY id
                )
                SELECT
                    raw.id,
                    raw.entity_id,
                    raw."schema",
                    raw.prop,
                    raw.value,
                    raw.dataset,
                    raw.lang,
                    raw.original_value,
                    raw.origin,
                    raw.external,
                    coalesce(previous.first_seen, raw.last_seen) AS first_seen,
                    raw.last_seen
                FROM raw
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
    """Dump the run's statements.pack from its statements.parquet.

    The parquet build is where statements get deduplicated and their
    ``first_seen`` computed, so dumping it gives pack consumers the
    deduplicated, correctly time-stamped view - the raw statements file
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
