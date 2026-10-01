import re
from collections import defaultdict
from html import unescape
from typing import Any
from urllib.parse import urljoin

from followthemoney.util import join_text

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)

PER_PAGE = 100
# A legislature is named for the years it runs, e.g. "2024 - 2028".
REGEX_TERM = re.compile(r"(\d{4})\s*-\s*(\d{4})")
IGNORE = [
    # Read from `_embedded`, where these carry term names rather than ids.
    "legislature",
    "mandats",
    "circonscriptions",
    "provinces",
    # Out of scope: `fonctions` separates deputies from substitutes, and `role-*` names
    # a role held in a group ("Membre"), never the group.
    "fonctions",
    "role-comite",
    "role-commission",
    "role-groupe-parlementaire",
    # WordPress plumbing.
    "date",
    "date_gmt",
    "modified",
    "modified_gmt",
    "guid",
    "slug",
    "status",
    "type",
    "featured_media",
    "class_list",
    "yoast_head",
    "yoast_head_json",
    "_links",
    "_embedded",
]


def parse_details(context: Context, person: Entity, profile_url: str) -> None:
    profile = context.fetch_html(profile_url, cache_days=7)
    # "Informations personnelles" and the mandate notes, as "Label : value" items.
    for item in h.xpath_elements(
        profile, '//span[@class="elementor-icon-list-text"][contains(., " : ")]'
    ):
        label, _, value = h.element_text(item).partition(" : ")
        field = context.lookup_value("details", label, warn_unmatched=True)
        if field == "birth_place":
            person.add("birthPlace", value)
        elif field == "birth_date":
            h.apply_date(person, "birthDate", value)
    # The party and parliamentary group widgets hold a linked heading, or "N/A".
    party_widget = h.xpath_element(profile, '//div[@data-id="0459684"]')
    for party in h.xpath_elements(party_widget, ".//h2"):
        person.add("political", h.element_text(party))


def crawl_member(
    context: Context,
    record: dict[str, Any],
    period_start: str,
    period_end: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:

    person = context.make("Person")
    person.id = context.make_slug(record.pop("id"))
    person.add("name", unescape(record.pop("title")["rendered"]).strip())
    person.add("citizenship", "cd")
    profile_url = record.pop("link")
    person.add("sourceUrl", profile_url)
    context.audit_data(record, ignore=IGNORE)

    profile = context.fetch_html(profile_url, cache_days=7)
    parse_details(context, person, profile_url)

    # Term names per taxonomy, e.g. {"provinces": ["Ituri"]}.
    taxonomies: dict[str, list[str]] = defaultdict(list)
    for terms in record.get("_embedded", {}).get("wp:term", []):
        for term in terms:
            taxonomies[term["taxonomy"]].append(unescape(term["name"]).strip())
    # Only ended and suspended mandates override the status; make_occupancy decides the rest.
    mandate = context.lookup_value("mandate", next(iter(taxonomies["mandats"]), None))
    status = OccupancyStatus(mandate) if mandate is not None else None
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=period_start,
        period_end=period_end,
        status=status,
    )
    if occupancy is None:
        return
    constituency = join_text(
        *taxonomies["circonscriptions"], *taxonomies["provinces"], sep=", "
    )
    occupancy.add("constituency", constituency)
    group_widget = h.xpath_element(profile, '//div[@data-id="976268c"]')
    for group in h.xpath_elements(group_widget, ".//h2"):
        occupancy.add("politicalGroup", h.element_text(group))
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
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Members are published against the sitting legislature, which the taxonomy hands
    # over as the newest term by name. A few sitting deputies were never tagged with it.
    terms = context.fetch_json(
        urljoin(context.data_url, "legislature"),
        params={"per_page": "1", "orderby": "name", "order": "desc"},
        cache_days=1,
    )
    term = REGEX_TERM.fullmatch(terms[0]["name"].strip())
    assert term is not None, terms[0]["name"]
    period_start, period_end = term.groups()

    # `offset`, not `page`: the API 400s past the last page. Uncached, because a
    # paginated listing shifts. The chamber seats 500, so 5000 bounds the loop.
    for offset in range(0, 5000, PER_PAGE):
        params = {"per_page": PER_PAGE, "_embed": 1, "offset": offset}
        data = context.fetch_json(context.data_url, params=params)
        for record in data:
            crawl_member(
                context, record, period_start, period_end, position, categorisation
            )
        if len(data) < PER_PAGE:
            break
    else:
        raise RuntimeError("Paging never reached a short page: is `offset` ignored?")
