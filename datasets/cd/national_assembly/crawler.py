from collections import Counter, defaultdict
from html import unescape
from itertools import count
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Term id of the current legislature ("2024 - 2028") in the source's WordPress taxonomy.
# A new term lands under a new id, so the crawler emits nothing and fails loudly below.
CURRENT_LEGISLATURE = "192"
# Mandate status ("mandats" taxonomy) of a deputy who is currently sitting. The source
# also carries "Terminé" (mandate ended, e.g. replaced by a substitute) and "Suspendu";
# those are not currently-serving members and are skipped (counted, not silently dropped).
STATUS_CURRENT = "En cours"
PER_PAGE = 100


def terms_by_taxonomy(record: dict[str, Any]) -> dict[str, list[str]]:
    """Map each embedded WordPress taxonomy to its term names for one deputy record."""
    out: dict[str, list[str]] = defaultdict(list)
    for group in record.get("_embedded", {}).get("wp:term", []):
        for term in group:
            out[term["taxonomy"]].append(term["name"])
    return out


def crawl_member(
    context: Context,
    record: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
    skipped: Counter[str],
) -> None:
    terms = terms_by_taxonomy(record)
    # Only currently-serving deputies (substitutes who have taken up a seat also show
    # "En cours"). Former/suspended mandates are recorded but not emitted here.
    if STATUS_CURRENT not in terms.get("mandats", []):
        skipped[", ".join(terms.get("mandats", [])) or "<none>"] += 1
        return

    name = unescape(record["title"]["rendered"]).strip()
    if len(name) == 0:
        return

    person = context.make("Person")
    person.id = context.make_slug("depute", str(record["id"]))
    person.add("name", name)
    # Deputies must be Congolese nationals (Constitution Art. 102(1): "être Congolais").
    # https://www.constituteproject.org/constitution/Democratic_Republic_of_the_Congo_2011
    person.add("citizenship", "cd")
    person.add("sourceUrl", record.get("link"))

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    for constituency in terms.get("circonscriptions", []):
        occupancy.add("constituency", constituency)
    for province in terms.get("provinces", []):
        occupancy.add("constituency", province)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of the Democratic Republic of the Congo",
        country="cd",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295979",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    fetched = 0
    skipped: Counter[str] = Counter()
    for page in count(1):
        data = context.fetch_json(
            context.data_url,
            params={
                "per_page": str(PER_PAGE),
                "_embed": "1",
                "legislature": CURRENT_LEGISLATURE,
                "page": str(page),
            },
            cache_days=1,
        )
        for record in data:
            crawl_member(context, record, position, categorisation, skipped)
        fetched += len(data)
        if len(data) < PER_PAGE:
            break
    if fetched == 0:
        raise ValueError(
            f"No deputies for legislature id {CURRENT_LEGISLATURE} — "
            "a new term may have started under a new taxonomy id."
        )
    context.log.info(
        "Skipped non-current mandates",
        by_status=dict(skipped),
        total_records=fetched,
    )
