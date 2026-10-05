"""Replace the contents of local zavod tables with dumps of the production ones.

The dumps are gzipped CSVs written daily by the etl-prod-dev-dumps job to the
prod-etl-dev-dumps bucket. The header row names the columns, which are matched
against the local table, so a load does not depend on column order.

The target database comes from ZAVOD_DATABASE_URI. Postgres and SQLite are
supported. The tables must already exist: run a crawler once to create them.

Run it from the zavod virtualenv, which has all the dependencies:

    python3 contrib/load_tables/load_tables.py reviews

or standalone, without activating anything:

    uv run --directory contrib/load_tables load_tables.py reviews
"""

import csv
import gzip
import os
import subprocess
from pathlib import Path
from typing import Any
from collections.abc import Callable, Iterable, Iterator

import click
from sqlalchemy import Boolean, Column, MetaData, Table, create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

BUCKET = "gs://prod-etl-dev-dumps.opensanctions.org"
# The bucket grants this service account rather than staff directly, and the
# crawler team may impersonate it, so downloads work from your own gcloud login
# without a key file. It is also the account the dev docs set up for reading
# production data.
IMPERSONATE = "etl-crawlerteam-sa@opensanctions-ops.iam.gserviceaccount.com"
DOWNLOAD_PATH = Path("data/dev-dumps")
RESOLVER_SOURCES = {"resolver": f"{BUCKET}/resolver/resolver.csv.gz"}
REVIEW_SOURCES = {
    "review": f"{BUCKET}/data_reviews/review.csv.gz",
    "review_entity": f"{BUCKET}/data_reviews/review_entity.csv.gz",
}
POSITION_SOURCES = {"position": f"{BUCKET}/positions/position.csv.gz"}


def get_engine() -> Engine:
    uri = os.environ.get("ZAVOD_DATABASE_URI")
    if uri is None:
        raise click.ClickException(
            "Set ZAVOD_DATABASE_URI to the database you want to load into."
        )
    try:
        url = make_url(uri)
    except Exception as exc:
        raise click.ClickException(f"Invalid ZAVOD_DATABASE_URI: {exc}")
    # Rendered without the password, so a password containing "prod" doesn't
    # trip the guard, and the URI in the message is safe to show.
    safe = url.render_as_string(hide_password=True)
    if "prod" in safe.lower():
        raise click.ClickException(f"Refusing to load into production: {safe}")
    backend = url.get_backend_name()
    if backend not in ("postgresql", "sqlite"):
        raise click.ClickException(f"Unsupported database: {backend}")
    return create_engine(url)


def reflect(engine: Engine, name: str) -> Table:
    if not inspect(engine).has_table(name):
        raise click.ClickException(
            f"Table {name} does not exist. Run a crawler once to create the tables."
        )
    return Table(name, MetaData(), autoload_with=engine)


def fetch(source: str) -> Path:
    if not source.startswith("gs://"):
        path = Path(source)
        if not path.is_file():
            raise click.ClickException(f"No such file: {path}")
        return path
    DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)
    path = DOWNLOAD_PATH / source.rsplit("/", 1)[-1]
    click.echo(f"Downloading {source} to {path}...")
    try:
        subprocess.run(
            [
                "gcloud",
                "storage",
                f"--impersonate-service-account={IMPERSONATE}",
                "cp",
                source,
                str(path),
            ],
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        raise click.ClickException(f"Could not download {source}: {exc}")
    return path


def read_header(path: Path) -> list[str]:
    with gzip.open(path, "rt", newline="") as fh:
        return next(csv.reader(fh))


def check_columns(table: Table, header: Iterable[str]) -> None:
    unknown = [name for name in header if name not in table.c]
    if len(unknown):
        raise click.ClickException(
            f"The dump has columns {', '.join(unknown)}, which {table.name} does not."
        )


def make_converter(column: Column[Any]) -> Callable[[str], Any]:
    """Read back what Postgres wrote: t/f for booleans, an empty field for NULL."""
    if isinstance(column.type, Boolean):
        return lambda value: None if value == "" else value == "t"
    if column.nullable:
        return lambda value: None if value == "" else value
    return lambda value: value


def load_postgres(engine: Engine, table: Table, path: Path, header: list[str]) -> None:
    columns = ", ".join(f'"{name}"' for name in header)
    connection = engine.raw_connection()
    try:
        driver: Any = connection.driver_connection
        cursor = driver.cursor()
        cursor.execute(f'TRUNCATE "{table.name}"')
        # COPY reads the gzip stream directly, without a temporary file.
        with gzip.open(path, "rb") as fh:
            sql = (
                f'COPY "{table.name}" ({columns}) FROM STDIN WITH (FORMAT csv, HEADER)'
            )
            with cursor.copy(sql) as copy:
                while chunk := fh.read(1024 * 1024):
                    copy.write(chunk)
        if "id" in table.c:
            # The dump keeps the ids, so move the sequence past them.
            cursor.execute(
                f"SELECT setval(pg_get_serial_sequence('{table.name}', 'id'), "
                f'COALESCE((SELECT MAX(id) FROM "{table.name}"), 1))'
            )
        driver.commit()
    finally:
        connection.close()


def load_sqlite(engine: Engine, table: Table, path: Path, header: list[str]) -> None:
    columns = ", ".join(f'"{name}"' for name in header)
    placeholders = ", ".join("?" * len(header))
    converters = [make_converter(table.c[name]) for name in header]
    connection = engine.raw_connection()
    try:
        driver: Any = connection.driver_connection
        driver.execute(f'DELETE FROM "{table.name}"')
        with gzip.open(path, "rt", newline="") as fh:
            reader = csv.reader(fh)
            next(reader)
            rows: Iterator[list[Any]] = (
                [convert(value) for convert, value in zip(converters, row)]
                for row in reader
            )
            sql = f'INSERT INTO "{table.name}" ({columns}) VALUES ({placeholders})'
            driver.executemany(sql, rows)
        driver.commit()
    finally:
        connection.close()


def count_rows(engine: Engine, name: str) -> int:
    with engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{name}"')).scalar_one()
    assert isinstance(count, int), name
    return count


def load(sources: dict[str, str], yes: bool) -> None:
    engine = get_engine()
    tables = {name: reflect(engine, name) for name in sources}
    click.echo(
        "About to replace the contents of these tables in "
        f"{engine.url.render_as_string(hide_password=True)}:"
    )
    for name, source in sources.items():
        click.echo(f"  {name}  <-  {source}")
    if not yes:
        click.confirm("Continue?", abort=True)
    for name, source in sources.items():
        table = tables[name]
        path = fetch(source)
        header = read_header(path)
        check_columns(table, header)
        click.echo(f"Loading {name}...")
        if engine.dialect.name == "postgresql":
            load_postgres(engine, table, path, header)
        else:
            load_sqlite(engine, table, path, header)
        click.echo(f"Loaded {count_rows(engine, name):,} rows into {name}.")


yes_option = click.option(
    "--yes", is_flag=True, default=False, help="Skip the confirmation prompt."
)


@click.group(help=__doc__)
def cli() -> None:
    pass


@cli.command("resolver", help="Load the resolver table.")
@yes_option
def resolver_command(yes: bool) -> None:
    load(RESOLVER_SOURCES, yes)


@cli.command("reviews", help="Load the data review tables.")
@yes_option
def reviews_command(yes: bool) -> None:
    load(REVIEW_SOURCES, yes)


@cli.command("positions", help="Load the PEP position table.")
@yes_option
def positions_command(yes: bool) -> None:
    load(POSITION_SOURCES, yes)


@cli.command("tables", help="Load named tables, e.g. review=data/review.csv.gz")
@click.argument("specs", nargs=-1, required=True)
@yes_option
def tables_command(specs: tuple[str, ...], yes: bool) -> None:
    sources: dict[str, str] = {}
    for spec in specs:
        name, separator, source = spec.partition("=")
        if not len(separator) or not len(name) or not len(source):
            raise click.BadParameter(f"Expected TABLE=SOURCE, got {spec}")
        sources[name] = source
    load(sources, yes)


if __name__ == "__main__":
    cli()
