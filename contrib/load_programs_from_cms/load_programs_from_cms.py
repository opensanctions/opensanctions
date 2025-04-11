import dataclasses
import json
import logging
import os
import sys

import click

import requests
from sqlalchemy import delete

from sqlalchemy.orm import Session

from zavod.db import get_engine
from zavod.logs import configure_logging
from zavod.stateful.model import Program, create_db

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

    with Session(get_engine()) as session:
        session.execute(delete(Program))
        session.add_all(
            [
                Program(
                    id=program["id"],
                    key=program["key"],
                    title=program["title"],
                    url=program["url"],
                )
                for program in programs
            ]
        )
        session.commit()
        log.info("Database now has %d programs", session.query(Program).count())


@cli.command
def dump_fixture():
    with Session(get_engine()) as session:
        sys.stdout.writelines(
            json.dumps(dataclasses.asdict(program)) + "\n"
            for program in session.query(Program).all()
        )


@cli.command
def load_fixture():
    with Session(get_engine()) as session:
        programs = [Program(**json.loads(line)) for line in sys.stdin]
        # Clear all programs before inserting new ones
        session.execute(delete(Program))
        session.add_all(programs)
        session.commit()
        log.info("Database now has %d programs", session.query(Program).count())


if __name__ == "__main__":
    cli()
