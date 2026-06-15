"""Debug CLI for the ``zavod.shed.wikidata`` entity-building helpers.

Provides two subcommands that run the same helpers used by the
``_wikidata/peps`` crawler against a single Wikidata QID and print the
resulting statements in the packed CSV format zavod writes to disk
during a real crawl. Nothing is written to the archive — the underlying
``Context`` is built in ``dry_run`` mode.

* ``human <QID>`` — runs ``wikidata_basic_human`` against the QID and,
  for every ``P39`` (position held) claim, runs ``wikidata_position`` +
  ``wikidata_occupancy``. Output covers the Person plus a Position and
  Occupancy for every accepted claim. Fails if the QID is not accepted
  as a person.
* ``position <QID>`` — runs ``wikidata_position`` against the QID.
  Output covers just the Position. Fails if the QID is not accepted as
  a position.

Useful for inspecting why a specific person/position is (or is not)
being included, or what its statements look like, without running the
full PEPs crawler. Common debugging flows:

* A person you'd expect in ``wd_peps`` is missing — run ``human`` to see
  whether ``wikidata_basic_human`` rejects them (returns ``None``) or
  whether every one of their ``P39`` claims is filtered out by
  ``wikidata_position`` / ``wikidata_occupancy``. Combine with
  ``wd_categories_paths.py`` in this directory to understand which
  Wikipedia category path got them included in the candidate set in the
  first place.
* A position looks wrong (label, country, topics, dates) — run
  ``position`` with that QID to see just the Position statements.
* A person from a sanctions/PEP source matches against ``wd_peps`` and
  the merged entity looks off — run ``human`` on their QID to see which
  positions/occupancies the helpers generated, and whether the dates
  and countries look right.

Usage (from the opensanctions repo root, so the default dataset path
resolves):

    python contrib/wikidata/wd_item_debug.py human Q567
    python contrib/wikidata/wd_item_debug.py position Q4970706
    python contrib/wikidata/wd_item_debug.py human Q567 | csvlens
    python contrib/wikidata/wd_item_debug.py -p datasets/_wikidata/peps/wd_peps.yml human Q567
"""

import logging
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import click
from followthemoney.statement.serialize import PackStatementWriter
from nomenklatura.wikidata import Item, WikidataClient
from rigour.ids.wikidata import is_qid

from zavod import settings
from zavod.context import Context
from zavod.entity import Entity
from zavod.logs import configure_logging
from zavod.meta import load_dataset_from_path
from zavod.shed.wikidata.human import wikidata_basic_human
from zavod.shed.wikidata.position import wikidata_occupancy, wikidata_position
from zavod.stateful.model import create_db


DEFAULT_DATASET = Path("datasets/_wikidata/peps/wd_peps.yml")


@dataclass
class RunContext:
    """Per-invocation state shared by every subcommand."""

    context: Context
    client: WikidataClient
    writer: PackStatementWriter
    dataset_name: str


@contextmanager
def _run_context(dataset_path: Path) -> Iterator[RunContext]:
    """Set up logging, DB, dataset, Context, WikidataClient and a stdout
    PackStatementWriter for the duration of one subcommand invocation.
    Closes the writer and the Context on exit so the CSV is fully flushed.
    """
    configure_logging(level=logging.INFO)
    create_db()
    dataset = load_dataset_from_path(dataset_path)
    if dataset is None:
        raise click.BadParameter(f"Invalid dataset path: {dataset_path}")
    context = Context(dataset, dry_run=True)
    context.begin(clear=False)
    writer = PackStatementWriter(sys.stdout)
    client = WikidataClient(context.cache, context.http)
    try:
        yield RunContext(
            context=context,
            client=client,
            writer=writer,
            dataset_name=dataset.name,
        )
    finally:
        writer.close()
        context.close()


def _emit(writer: PackStatementWriter, dataset_name: str, entity: Entity) -> None:
    """Replicate the statement-cloning side of ``Context.emit`` but stream
    each statement to the given writer instead of the on-disk pack file."""
    for stmt in entity.statements:
        stmt = stmt.clone(
            dataset=dataset_name,
            entity_id=entity.id,
            schema=entity.schema.name,
            lang=stmt._lang,
            origin=stmt.origin,
            external=False,
        )
        stmt.first_seen = settings.RUN_TIME_ISO
        stmt.last_seen = settings.RUN_TIME_ISO
        writer.write(stmt)


def _fetch_item(client: WikidataClient, qid: str) -> Item:
    if not is_qid(qid):
        raise click.BadParameter(f"Not a valid Wikidata QID: {qid}")
    item = client.fetch_item(qid)
    if item is None:
        raise click.ClickException(f"Wikidata item not found: {qid}")
    return item


@click.group(help="Debug CLI for the zavod.shed.wikidata entity-building helpers.")
@click.option(
    "-p",
    "--dataset-path",
    type=click.Path(dir_okay=False, readable=True, path_type=Path),
    default=DEFAULT_DATASET,
    show_default=True,
    help="Dataset YAML used to seed the Context (lookups, cache, http).",
)
@click.pass_context
def main(ctx: click.Context, dataset_path: Path) -> None:
    ctx.obj = dataset_path


@main.command(
    "human",
    help="Emit statements for a Wikidata human (Person + P39 positions + occupancies).",
)
@click.argument("qid")
@click.pass_obj
def cmd_human(dataset_path: Path, qid: str) -> None:
    with _run_context(dataset_path) as run:
        item = _fetch_item(run.client, qid)
        person = wikidata_basic_human(run.context, run.client, item)
        if person is None:
            raise click.ClickException(f"QID {qid} is not a relevant person.")

        _emit(run.writer, run.dataset_name, person)
        for claim in item.claims:
            if claim.property != "P39" or claim.qid is None:
                continue
            pos_item = run.client.fetch_item(claim.qid)
            if pos_item is None:
                continue
            position = wikidata_position(run.context, run.client, pos_item)
            if position is None:
                continue
            occupancy = wikidata_occupancy(run.context, person, position, claim)
            if occupancy is None:
                continue
            _emit(run.writer, run.dataset_name, position)
            _emit(run.writer, run.dataset_name, occupancy)


@main.command("position", help="Emit statements for a Wikidata position.")
@click.argument("qid")
@click.pass_obj
def cmd_position(dataset_path: Path, qid: str) -> None:
    with _run_context(dataset_path) as run:
        item = _fetch_item(run.client, qid)
        position = wikidata_position(run.context, run.client, item)
        if position is None:
            raise click.ClickException(f"QID {qid} is not a relevant position.")
        _emit(run.writer, run.dataset_name, position)


if __name__ == "__main__":
    main()
