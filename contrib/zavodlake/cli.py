import logging

import click
import duckdb

from zavod import settings
from zavod.logs import configure_logging, get_logger
from zavod.meta import load_directory_catalog

from contrib.zavodlake.convert import convert_dataset
from contrib.zavodlake.fetch import fetch_dataset

log = get_logger("zavodlake")

# Datasets whose newest archived statements.pack predates the current pack
# format (headerless, no id column). Skipped rather than crashing the build;
# remove once they are re-exported or leave the scope collection.
SKIP_DATASETS = {"lt_pep_declarations"}


@click.group(help="Parquet statement lake prototyping workbench")
def cli() -> None:
    pass


@cli.command("build", help="Backfill packs and convert them to parquet")
@click.argument("datasets", nargs=-1)
@click.option(
    "--scope",
    "scope_name",
    default="default",
    show_default=True,
    help="Collection whose leaf datasets are processed",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Re-convert parquet files even if they are up to date",
)
def build(datasets: tuple[str, ...], scope_name: str, force: bool) -> None:
    configure_logging(level=logging.INFO)
    if settings.ARCHIVE_BACKEND == "FileSystemBackend" and (
        settings.ARCHIVE_BUCKET is None
    ):
        log.info("Archive not configured, defaulting to production bucket")
        settings.ARCHIVE_BACKEND = "GoogleCloudBackend"
        settings.ARCHIVE_BUCKET = "data.opensanctions.org"

    catalog = load_directory_catalog()
    scope = catalog.require(scope_name)
    leaves = sorted(scope.leaves, key=lambda d: d.name)
    if len(datasets) > 0:
        unknown = set(datasets) - {leaf.name for leaf in leaves}
        if len(unknown) > 0:
            raise click.BadParameter(
                f"Not leaf datasets of {scope_name!r}: {', '.join(sorted(unknown))}"
            )
        leaves = [leaf for leaf in leaves if leaf.name in set(datasets)]

    conn = duckdb.connect()
    temp_path = settings.DATA_PATH / "lake" / ".duckdb_tmp"
    conn.execute(f"SET temp_directory = '{temp_path}'")
    converted, fresh, missing, skipped = 0, 0, 0, 0
    for leaf in leaves:
        if leaf.name in SKIP_DATASETS:
            log.info("Skipping dataset with unsupported pack", dataset=leaf.name)
            skipped += 1
            continue
        pack_path = fetch_dataset(leaf.name)
        if pack_path is None:
            log.warning("No statements.pack in the archive", dataset=leaf.name)
            missing += 1
            continue
        result = convert_dataset(conn, leaf.name, pack_path, force=force)
        if result is None:
            log.info("Parquet is up to date", dataset=leaf.name)
            fresh += 1
            continue
        rows_in, rows_out = result
        log.info(
            "Converted to parquet",
            dataset=leaf.name,
            rows_in=rows_in,
            rows_out=rows_out,
            duplicates=rows_in - rows_out,
        )
        converted += 1
    log.info(
        "Lake build complete",
        scope=scope_name,
        converted=converted,
        fresh=fresh,
        missing=missing,
        skipped=skipped,
    )


if __name__ == "__main__":
    cli()
