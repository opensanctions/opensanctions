import re
from collections import defaultdict
from dataclasses import dataclass
from html import unescape
from itertools import count
from typing import Any
from urllib.parse import urljoin

from rigour.dates import ended_before

from zavod import Context, settings
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)

TOPICS = ["gov.national", "gov.legislative"]
PER_PAGE = 100


@dataclass(frozen=True)
class Term:
    """A legislature as published in the source's `legislature` taxonomy.

    The taxonomy only names the years a legislature ran, so its bounds are
    year-precision dates shared by everyone who served in it.
    """

    id: int
    name: str
    period_start: str
    period_end: str

    @property
    def has_ended(self) -> bool:
        """Whether the whole legislature lies in the past."""
        return ended_before(self.period_end, settings.RUN_TIME)


def crawl_terms(context: Context) -> list[Term]:
    """List the legislatures the source publishes, newest first."""
    # Sibling endpoint of data.url in the same WordPress REST namespace.
    url = urljoin(context.data_url, "legislature")
    terms: list[Term] = []
    for record in context.fetch_json(
        url, params={"per_page": str(PER_PAGE)}, cache_days=1
    ):
        name = unescape(record["name"]).strip()
        # Every occupancy is dated from the legislature's name, so a name that is not
        # a year range means the term bounds can no longer be derived.
        match = re.fullmatch(r"(\d{4})\s*-\s*(\d{4})", name)
        if match is None:
            raise ValueError(f"Unexpected legislature name: {name!r}")
        terms.append(Term(record["id"], name, match.group(1), match.group(2)))
    if len(terms) == 0:
        raise ValueError("The legislature taxonomy is empty.")
    return sorted(terms, key=lambda term: term.period_start, reverse=True)


def terms_by_taxonomy(record: dict[str, Any]) -> dict[str, list[str]]:
    """Map each embedded WordPress taxonomy to its term names for one deputy record."""
    out: dict[str, list[str]] = defaultdict(list)
    for group in record.get("_embedded", {}).get("wp:term", []):
        for term in group:
            out[term["taxonomy"]].append(term["name"])
    return out


def mandate_status(
    context: Context, term: Term, mandate: str | None
) -> OccupancyStatus | None:
    """Map the source's mandate status onto an occupancy status override.

    The `mandats` taxonomy states whether a deputy's mandate is running, has ended or
    is suspended, but never when — the one case where the status derived from the dates
    should be overridden. For a legislature that is already over, the term bounds are
    the firmer fact and decide continued political exposure, so nothing is overridden
    there.
    """
    if term.has_ended or mandate is None:
        return None
    status = context.lookup_value("mandate_status", mandate)
    if status is None:
        raise ValueError(f"Unknown mandate status: {mandate!r}")
    if status == "current":
        return OccupancyStatus.CURRENT
    if status == "ended":
        return OccupancyStatus.ENDED
    if status == "unknown":
        return None
    raise ValueError(f"Unexpected mandate_status lookup result: {status!r}")


def crawl_member(
    context: Context,
    record: dict[str, Any],
    term: Term,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    name = unescape(record["title"]["rendered"]).strip()
    if len(name) == 0:
        return
    taxonomies = terms_by_taxonomy(record)
    mandates = taxonomies.get("mandats", [])
    if len(mandates) > 1:
        raise ValueError(f"Deputy {record['id']} has several mandates: {mandates}")

    person = context.make("Person")
    person.id = context.make_slug("depute", str(record["id"]))
    person.add("name", name)
    # Deputies must be Congolese nationals (Constitution Art. 102(1): "être Congolais").
    # https://www.constituteproject.org/constitution/Democratic_Republic_of_the_Congo_2011
    person.add("citizenship", "cd")
    person.add("sourceUrl", record["link"])

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=term.period_start,
        period_end=term.period_end,
        no_end_implies_current=False,
        status=mandate_status(context, term, mandates[0] if mandates else None),
    )
    if occupancy is None:
        return
    for constituency in taxonomies.get("circonscriptions", []):
        occupancy.add("constituency", constituency)
    for province in taxonomies.get("provinces", []):
        occupancy.add("constituency", province)
    context.emit(occupancy)
    context.emit(person)


def crawl_term(
    context: Context,
    term: Term,
    position: Entity,
    categorisation: PositionCategorisation,
) -> int:
    """Emit the deputies recorded for one legislature, returning the record count."""
    records = 0
    for page in count(1):
        data = context.fetch_json(
            context.data_url,
            params={
                "per_page": str(PER_PAGE),
                "_embed": "1",
                "legislature": str(term.id),
                "page": str(page),
            },
            cache_days=1,
        )
        for record in data:
            crawl_member(context, record, term, position, categorisation)
        records += len(data)
        if len(data) < PER_PAGE:
            break
    return records


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of the Democratic Republic of the Congo",
        country="cd",
        topics=TOPICS,
        wikidata_id="Q21295979",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    terms = crawl_terms(context)
    counts: dict[str, int] = {}
    for index, term in enumerate(terms):
        if term.period_end < h.earliest_term_start(TOPICS):
            context.log.info(
                "Legislatures predate the PEP window; skipping",
                legislatures=[skipped.name for skipped in terms[index:]],
            )
            break
        counts[term.name] = crawl_term(context, term, position, categorisation)
    # The sitting legislature always has deputies. If it doesn't, the post type or the
    # taxonomy filter has changed and the crawler is no longer reading the roster.
    if counts.get(terms[0].name, 0) == 0:
        raise ValueError(
            f"Legislature {terms[0].name!r} (id {terms[0].id}) has no deputies — "
            "the structure of the source may have changed."
        )
    context.log.info("Crawled legislatures", records=counts)
