import dataclasses
import json
import logging
import os
import sys

import click
import requests
from sqlalchemy import select
from sqlalchemy.sql.functions import count

from zavod.db import get_engine
from zavod.logs import configure_logging
from zavod.stateful.model import create_db, program_table
from zavod.stateful.programs import Program

DIRECTUS_TOKEN = os.environ.get("ZAVOD_DIRECTUS_TOKEN")

DIRECTUS_PROGRAMS_URL = "https://opensanctions.directus.app/items/programs"

log = logging.getLogger(__name__)


@click.group(help="Load Programs from CMS")
def cli():
    configure_logging(level=logging.INFO)
    create_db()


@cli.command
def load():
    response = requests.request(
        "SEARCH",
        DIRECTUS_PROGRAMS_URL,
        json={"query": {"filter": {"status": {"_eq": "published"}}, "limit": -1}},
        headers={"Authorization": f"Bearer {DIRECTUS_TOKEN}"},
    )
    programs = response.json()["data"]
    log.info("Found %d programs in Directus", len(programs))
    with get_engine().connect() as conn:
        with conn.begin():
            conn.execute(program_table.delete())
            for program in programs:
                conn.execute(
                    program_table.insert().values(
                        id=program["id"],
                        key=program["key"],
                        title=program["title"],
                        url=program["url"],
                    )
                )

        log.info(
            "Database now has %d programs",
            conn.execute(select(count()).select_from(program_table)).scalar(),
        )


@cli.command
def dump_fixture():
    with get_engine().connect() as conn:
        sys.stdout.writelines(
            json.dumps(
                dataclasses.asdict(
                    Program(id=row.id, key=row.key, title=row.title, url=row.url)
                )
            )
            + "\n"
            for row in conn.execute(program_table.select()).fetchall()
        )


@cli.command
def load_fixture():
    programs = [Program(**json.loads(line)) for line in sys.stdin]

    with get_engine().connect() as conn:
        with conn.begin():
            conn.execute(program_table.delete())
            for program in programs:
                conn.execute(program_table.insert().values(dataclasses.asdict(program)))
        log.info(
            "Database now has %d programs",
            conn.execute(select(count()).select_from(program_table)).scalar(),
        )


if __name__ == "__main__":
    cli()
