import csv
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import click
from nomenklatura.wikidata import Claim, Item
from nomenklatura.wikidata.value import clean_wikidata_name
from rigour.time import iso_datetime
from zavod.archive import clear_data_path
from zavod.logs import configure_logging
from zavod.meta import Dataset, load_dataset_from_path
from zavod.shed.wikidata.client import WIKIDATA_QUERY_CACHE, create_wikidata_client
from zavod.shed.wikidata.human import wikidata_basic_human
from zavod.shed.wikidata.position import (
    wikidata_occupancy,
    wikidata_position,
)
from zavod.stateful.positions import categorised_position_qids

from zavod import Context, Entity
from zavod import helpers as h

DATASET_PATH = Path(__file__).parent / "wd_categories.yml"
URL = "https://petscan.wmcloud.org/"
QUERY = {
    "doit": "",
    "depth": 4,
    # "combination": "subset",
    "format": "csv",
    "wikidata_item": "with",
    "wikidata_prop_item_use": "Q5",
    "search_max_results": 1000,
    "sortorder": "ascending",
}
# That one time a PEP customer asked to be included....
ALWAYS_PERSONS = ["Q21258544"]


@dataclass
class FoundRecord:
    from_categories: set[str] = field(default_factory=set)
    from_declarator: bool = False


class CrawlState:
    def __init__(self, context: Context):
        self.context = context
        self.client = create_wikidata_client(context)
        self.log = context.log
        # Position QID -> evaluated position entity, None if the item is not
        # a usable PEP position. Positions recur across the whole person set,
        # so each distinct QID is fetched and categorised only once per run.
        self.positions: dict[str, Entity | None] = {}
        blocked = 0
        for qid, is_pep in categorised_position_qids(context):
            if not is_pep:
                self.positions[qid] = None
                blocked += 1
        self.log.info("Preloaded rejected positions", blocked=blocked)

        self.persons: dict[str, FoundRecord] = defaultdict(FoundRecord)
        self.persons.update({qid: FoundRecord() for qid in ALWAYS_PERSONS})

        self.person_title: dict[str, str] = {}
        self.person_countries: dict[str, set[str]] = {}
        self.person_topics: dict[str, set[str]] = {}
        self.person_positions: dict[str, set[Entity]] = {}
        self._emitted_positions: set[str] = set()
        self._crawled_officeholder_positions: set[str] = set()
        exc = [str(x) for x in context.dataset.config.get("exclusion_checks", [])]
        self.exclusion_checks: set[str] = set(exc)
        excl = [str(x) for x in context.dataset.config.get("excluded_qids", [])]
        self.excluded_qids: set[str] = set(excl)
        self.excluded_qids_seen: set[str] = set()
        self.person_modified_at: dict[str, datetime] = {}

    def emit_position(self, position: Entity) -> None:
        if position.id is None:
            return
        if position.id not in self._emitted_positions:
            self._emitted_positions.add(position.id)
            self.context.emit(position)


def title_name(title: str) -> str | None:
    return clean_wikidata_name(title.replace("_", " "))


def get_position(state: CrawlState, qid: str) -> Entity | None:
    """Reuse evaluated positions across people who hold the same office."""
    if qid in state.positions:
        return state.positions[qid]
    item = state.client.fetch_item(qid)
    if item is None:
        state.positions[qid] = None
        return None
    position = wikidata_position(state.context, state.client, item)
    if position is None or position.id is None:
        state.positions[qid] = None
        state.positions[item.id] = None
        return None
    if item.id != qid:
        state.context.resolver.rename_node(qid, item.id)
        state.context.flush()
    state.positions[qid] = position
    state.positions[item.id] = position
    return position


def crawl_officeholders(state: CrawlState, item: Item, position: Entity) -> None:
    if position.id is None or position.id in state._crawled_officeholder_positions:
        return
    state._crawled_officeholder_positions.add(position.id)

    # persons via position --( P1308 officeholder )--> person
    for claim in item.claims:
        if claim.property != "P1308" or claim.qid is None:
            continue
        holder = crawl_person(state, claim.qid, recurse=False)
        if holder is None:
            continue
        occupancy = wikidata_occupancy(state.context, holder, position, claim)
        if occupancy is not None:
            state.emit_position(position)
            state.context.emit(occupancy)
            state.context.emit(holder)


def crawl_position(state: CrawlState, person: Entity, claim: Claim) -> None:
    if claim.qid is None:
        return
    position = get_position(state, claim.qid)
    if position is None:
        return
    item = state.client.fetch_item(claim.qid)
    if item is not None:
        crawl_officeholders(state, item, position)
    occupancy = wikidata_occupancy(state.context, person, position, claim)
    if occupancy is not None:
        state.log.info("  -> {} ({})".format(position.first("name"), position.id))
        state.emit_position(position)
        state.context.emit(occupancy)


def crawl_person(state: CrawlState, qid: str, recurse: bool = True) -> Entity | None:
    modified_at = state.person_modified_at.get(qid)
    item = state.client.fetch_item(qid, modified_at=modified_at)
    if item is None:
        return None
    if item.id != qid:
        state.context.resolver.rename_node(qid, item.id)
        state.context.flush()
    entity = wikidata_basic_human(state.context, state.client, item, strict=True)
    if entity is None:
        return None

    if recurse:
        for claim in item.claims:
            if claim.property == "P39":
                crawl_position(state, entity, claim)
    return entity


def crawl_category(state: CrawlState, category_crawl_spec: dict[str, Any]) -> None:
    topics: list[str] = category_crawl_spec.pop("topics", [])
    if "topic" in category_crawl_spec:
        topics.append(category_crawl_spec.pop("topic"))
    country: str | None = category_crawl_spec.pop("country", None)

    query = dict(QUERY)
    cat: str = category_crawl_spec.pop("category", "")
    query["categories"] = cat.strip()
    query.update(category_crawl_spec)
    state.log.info(f"Crawl category: {cat}")

    position_data: dict[str, Any] = category_crawl_spec.pop("position", {})
    position: Entity | None = None
    if "name" in position_data:
        position = h.make_position(
            state.context,
            **position_data,
            # Our position specs in the metadata are always in English
            lang="eng",
            id_hash_prefix="wd-cat",
        )

    query_string = urlencode(query)
    url = f"{URL}?{query_string}"
    data = state.context.fetch_text(url, cache_days=WIKIDATA_QUERY_CACHE)
    wrapper = StringIO(data)
    results = 0
    for row in csv.DictReader(wrapper):
        results += 1
        person_qid = row["Wikidata"]
        if person_qid is None:
            continue
        if person_qid in state.exclusion_checks:
            state.context.log.warning(
                "Regression on exclusion found", category=cat, qid=person_qid
            )
            continue
        if person_qid in state.excluded_qids:
            state.excluded_qids_seen.add(person_qid)
            continue
        state.persons[person_qid].from_categories.add(cat)

        if person_qid not in state.person_topics:
            state.person_topics[person_qid] = set()
        state.person_topics[person_qid].update(topics)
        if person_qid not in state.person_countries:
            state.person_countries[person_qid] = set()
        if country is not None:
            state.person_countries[person_qid].add(country)
        if person_qid not in state.person_positions:
            state.person_positions[person_qid] = set()
        if position is not None:
            state.person_positions[person_qid].add(position)

        person_title = row.get("title")
        if isinstance(person_title, str):
            state.person_title[person_qid] = person_title

    state.log.info(
        f"PETScanning category: {cat}",
        topics=topics,
        results=results,
    )
    state.context.flush()


def crawl_declarator(state: CrawlState) -> None:
    # Import all profiles which have a Declarator ID, a reference to a Russian PEP
    # site containing profiles of all elected officials in Russia.
    query = """
    SELECT ?person ?modifiedAt WHERE {
        ?person wdt:P1883 ?value .
        ?person wdt:P31 wd:Q5 .
        ?person schema:dateModified ?modifiedAt .
    }
    """
    response = state.client.query(query, cache_days=WIKIDATA_QUERY_CACHE)
    state.log.info(f"Found {len(response.results)} declarator profiles")
    for result in response.results:
        person_qid = result.plain("person")
        if person_qid is None:
            continue
        modified_at = result.plain("modifiedAt")
        if modified_at is not None:
            modified_datetime = iso_datetime(modified_at)
            if modified_datetime is not None:
                state.person_modified_at[person_qid] = modified_datetime
        state.persons[person_qid] = FoundRecord(from_declarator=True)
        if person_qid not in state.person_topics:
            state.person_topics[person_qid] = set()
        state.person_topics[person_qid].add("role.pep")
        if person_qid not in state.person_countries:
            state.person_countries[person_qid] = set()
        state.person_countries[person_qid].add("ru")


def crawl_persons(state: CrawlState) -> None:
    state.context.log.info(f"Generated {len(state.persons)} persons")
    for idx, (person_qid, found_record) in enumerate(state.persons.items()):
        entity = crawl_person(state, person_qid)
        if entity is None:
            continue

        if not entity.has("name") and person_qid in state.person_title:
            name = title_name(state.person_title[person_qid])
            entity.add("name", name)
        entity.add("topics", state.person_topics.get(person_qid, []))

        if not len(entity.countries):
            entity.add("country", state.person_countries.get(person_qid, []))

        positions: set[Entity] = state.person_positions.get(person_qid, set())
        for position in positions:
            if position.id is None:
                continue
            occupancy = h.make_occupancy(
                state.context, entity, position, no_end_implies_current=False
            )
            if occupancy is not None:
                state.emit_position(position)
                state.context.emit(occupancy)

        state.log.info(
            f"Crawled person {entity.id} "
            f"(found in categories {found_record.from_categories}): "
            f"{entity.caption} {entity.get('topics')}"
        )

        # Some categories we crawl but don't assign topics to. We do this for two reasons:
        #   1. We only want to discover interesting positions in the category (which then trigger at
        #      topic assignment), but there are more persons in the category not relevant to us.
        #   2. We want to discover QIDs for enrichment, just in case the Wikidata enricher (which only searches
        #      for a single name) doesn't find them. In this case, the xref process will discover the QID for the
        #      cluster (but not actually pull in the statements marked as external).
        #  In both cases, we want to emit the statements as external.
        has_topics = len(entity.get("topics")) > 0
        state.context.emit(entity, external=(not has_topics))

        if idx > 0 and idx % 1000 == 0:
            state.context.flush()

    # raise RuntimeError("Crawler is in debug mode, do not release results")


def category_crawl_specs(dataset: Dataset) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = dataset.config.get("categories", [])
    return specs


def warn_stale_exclusions(state: CrawlState) -> None:
    """Report configured exclusions which no category produced.

    Only meaningful after a full run - a partial run skips most categories, so every
    exclusion not covered by the selected ones would look stale.
    """
    stale = state.excluded_qids - state.excluded_qids_seen
    if len(stale) > 0:
        state.context.log.warning(
            "Excluded QIDs no longer found in any category", qids=sorted(stale)
        )


def crawl(context: Context) -> None:
    state = CrawlState(context)
    crawl_declarator(state)
    for category_crawl_spec in category_crawl_specs(context.dataset):
        crawl_category(state, category_crawl_spec)
        state.context.flush()

    warn_stale_exclusions(state)
    crawl_persons(state)


@click.command()
@click.argument("categories", nargs=-1, required=True)
@click.option(
    "--clear/--keep",
    default=True,
    help="Clear the dataset output directory before running.",
)
@click.option(
    "-d",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Log what would be emitted without writing statements.",
)
@click.option("--debug", is_flag=True, default=False)
def cli(
    categories: tuple[str, ...],
    clear: bool = True,
    dry_run: bool = False,
    debug: bool = False,
) -> None:
    """Crawl only the named entries of the wd_categories category config.

    A full run takes hours, which makes it useless for checking what one category
    contributes, or what a `negcats` or `excluded_qids` change does to it. This runs the
    real crawler against the real dataset config, but over the given categories only.
    Declarator discovery is skipped.

    Output lands in data/datasets/wd_categories/ like a normal run, so it replaces
    whatever a previous local run left there unless you pass --keep.

    CATEGORIES - one or more `category` values as spelled in wd_categories.yml, e.g.
    Political_office-holders_in_the_United_States
    """
    configure_logging(level=logging.DEBUG if debug else logging.INFO)
    dataset = load_dataset_from_path(DATASET_PATH)
    if dataset is None:
        raise click.ClickException(f"Cannot load dataset: {DATASET_PATH}")

    specs = {s["category"].strip(): s for s in category_crawl_specs(dataset)}
    unknown = [c for c in categories if c not in specs]
    if len(unknown) > 0:
        raise click.BadParameter(
            f"Not in the category config: {', '.join(unknown)}",
            param_hint="CATEGORIES",
        )

    if clear and not dry_run:
        clear_data_path(dataset.name)

    context = Context(dataset, dry_run=dry_run)
    try:
        context.begin(clear=clear)
        state = CrawlState(context)
        for category in categories:
            crawl_category(state, specs[category])
            context.flush()
        crawl_persons(state)
        context.flush()
        context.finalize_statements()
    finally:
        context.close()


if __name__ == "__main__":
    cli()
