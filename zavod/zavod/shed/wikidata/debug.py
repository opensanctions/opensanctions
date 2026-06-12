"""Debug CLI for the ``zavod.shed.wikidata`` entity-building helpers.

Given a single Wikidata QID, this runs the same helpers that the
``_wikidata/peps`` crawler uses (``wikidata_basic_human``,
``wikidata_position``, ``wikidata_occupancy``) and prints the statements
that would be emitted, in the packed CSV format zavod writes to disk
during a real crawl. Nothing is written to the archive — the underlying
``Context`` is built in ``dry_run`` mode.

Useful for inspecting why a specific person/position is (or is not) being
included, or what its statements look like, without running the full PEPs
crawler.

For a person QID, the output covers the Person plus the Position and
Occupancy for every ``P39`` (position held) claim. For a position QID,
the output covers just the Position. For anything the helpers reject
(historical, fictional, too old, no country, ...) the tool exits with a
"neither a relevant person nor a position" error.

Usage:
    python -m zavod.shed.wikidata.debug Q567
    python -m zavod.shed.wikidata.debug Q4970706
    python -m zavod.shed.wikidata.debug Q567 -p datasets/_wikidata/peps/wd_peps.yml

Run from the opensanctions repo root so the default dataset path resolves.
"""

import logging
import sys
from pathlib import Path

import click
from followthemoney.statement.serialize import PackStatementWriter
from nomenklatura.wikidata import WikidataClient
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


@click.command(help="Print the statements emitted for a single Wikidata QID.")
@click.argument("qid")
@click.option(
    "-p",
    "--dataset-path",
    type=click.Path(dir_okay=False, readable=True, path_type=Path),
    default=DEFAULT_DATASET,
    show_default=True,
    help="Dataset YAML used to seed the Context (lookups, cache, http).",
)
def main(qid: str, dataset_path: Path) -> None:
    if not is_qid(qid):
        raise click.BadParameter(f"Not a valid Wikidata QID: {qid}")

    configure_logging(level=logging.INFO)
    create_db()

    dataset = load_dataset_from_path(dataset_path)
    if dataset is None:
        raise click.BadParameter(f"Invalid dataset path: {dataset_path}")

    context = Context(dataset, dry_run=True)
    context.begin(clear=False)
    writer = PackStatementWriter(sys.stdout)
    try:
        client = WikidataClient(context.cache, context.http)
        item = client.fetch_item(qid)
        if item is None:
            raise click.ClickException(f"Wikidata item not found: {qid}")

        person = wikidata_basic_human(context, client, item)
        if person is not None:
            _emit(writer, dataset.name, person)
            for claim in item.claims:
                if claim.property != "P39" or claim.qid is None:
                    continue
                pos_item = client.fetch_item(claim.qid)
                if pos_item is None:
                    continue
                position = wikidata_position(context, client, pos_item)
                if position is None:
                    continue
                occupancy = wikidata_occupancy(context, person, position, claim)
                if occupancy is None:
                    continue
                _emit(writer, dataset.name, position)
                _emit(writer, dataset.name, occupancy)
            return

        position = wikidata_position(context, client, item)
        if position is not None:
            _emit(writer, dataset.name, position)
            return

        raise click.ClickException(
            f"QID {qid} is neither a relevant person nor a position."
        )
    finally:
        writer.close()
        context.close()


if __name__ == "__main__":
    main()
