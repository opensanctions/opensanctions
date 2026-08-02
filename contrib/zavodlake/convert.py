from pathlib import Path

import duckdb

from zavod import settings

# Column order written by followthemoney's PackStatementWriter. Older, headerless
# pack files use a different column set and are deliberately not supported.
PACK_COLUMNS = [
    "entity_id",
    "prop",
    "value",
    "dataset",
    "lang",
    "original_value",
    "origin",
    "external",
    "first_seen",
    "last_seen",
    "id",
]
PACK_HEADER = ",".join(PACK_COLUMNS)


def dataset_parquet_path(dataset_name: str) -> Path:
    return settings.DATA_PATH / "lake" / dataset_name / "statements.parquet"


def _sql_str(value: Path) -> str:
    return str(value).replace("'", "''")


def _check_pack_header(pack_path: Path) -> None:
    with open(pack_path, "r", encoding="utf-8") as fh:
        first_line = fh.readline().rstrip("\n")
    if first_line != PACK_HEADER:
        raise ValueError(
            f"Unexpected statements.pack header in {pack_path}: {first_line!r}"
        )


def convert_dataset(
    conn: duckdb.DuckDBPyConnection,
    dataset_name: str,
    pack_path: Path,
    force: bool = False,
) -> tuple[int, int] | None:
    """Convert a dataset's statements.pack into a typed, deduplicated parquet file.

    Statements are deduplicated on the ``id`` column and sorted by ``entity_id``;
    the packed ``Schema:prop`` column is split into separate ``schema`` and
    ``prop`` columns. Returns (rows_in, rows_out), or None if the existing
    parquet file is newer than the pack file and ``force`` is not set.
    """
    out_path = dataset_parquet_path(dataset_name)
    if not force and out_path.is_file():
        if out_path.stat().st_mtime >= pack_path.stat().st_mtime:
            return None
    _check_pack_header(pack_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Pin the pack dialect (csv.unix_dialect) rather than letting duckdb sniff it:
    # a large unquoted sample makes the sniffer detect "no quoting", which then
    # breaks on the first quoted field further into the file. Single fields can be
    # up to PROP_VALUE_MAX (30 MB), far beyond duckdb's default max_line_size.
    select = f"""
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
        FROM read_csv('{_sql_str(pack_path)}', header = true, all_varchar = true,
            delim = ',', quote = '"', escape = '"', max_line_size = 33554432)
    """
    row = conn.execute(
        f"""
        SELECT count(*),
            count(*) FILTER (WHERE "schema" = '' OR prop = '' OR id IS NULL)
        FROM ({select})
        """
    ).fetchone()
    assert row is not None
    rows_in, bad_rows = int(row[0]), int(row[1])
    if bad_rows > 0:
        raise ValueError(
            f"{pack_path} has {bad_rows} rows with a malformed prop or a null id"
        )

    tmp_path = out_path.with_suffix(".parquet.tmp")
    copy_sql = f"""
        COPY (
            SELECT * FROM ({select})
            -- Rows can share an id with differing content: the statement id hash
            -- covers dataset, entity_id, prop, value, lang and external, so only
            -- schema, original_value, origin and the seen timestamps can differ
            -- within an id. Prefer the richest row, then order for determinism.
            QUALIFY row_number() OVER (
                PARTITION BY id
                ORDER BY original_value NULLS LAST, origin NULLS LAST,
                    "schema", first_seen, last_seen
            ) = 1
            ORDER BY entity_id
        ) TO '{_sql_str(tmp_path)}' (FORMAT parquet, COMPRESSION zstd)
    """
    try:
        out_row = conn.execute(copy_sql).fetchone()
        assert out_row is not None
        rows_out = int(out_row[0])
        tmp_path.replace(out_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return rows_in, rows_out
