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


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    period_start: str,
    period_end: str,
    record: dict[str, Any],
) -> None:
    # Term names per taxonomy, e.g. {"provinces": ["Ituri"]}.
    taxonomies: dict[str, list[str]] = defaultdict(list)
    for terms in record.get("_embedded", {}).get("wp:term", []):
        for term in terms:
            taxonomies[term["taxonomy"]].append(unescape(term["name"]).strip())

    person = context.make("Person")
    person.id = context.make_slug(record.pop("id"))
    person.add("name", unescape(record.pop("title")["rendered"]).strip())
    person.add("citizenship", "cd")
    profile_url = record.pop("link")
    person.add("sourceUrl", profile_url)

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
    # The party and group widgets hold a linked heading, or "N/A"; selecting the
    # widget itself fails loudly if the page template changes.
    party_widget = h.xpath_element(profile, '//div[@data-id="0459684"]')
    person.add("political", h.xpath_strings(party_widget, ".//h2//text()"))
    group_widget = h.xpath_element(profile, '//div[@data-id="976268c"]')
    political_groups = h.xpath_strings(group_widget, ".//h2//text()")

    # Only ended and suspended mandates override the status; make_occupancy decides the rest.
    mandate = context.lookup_value("mandate", next(iter(taxonomies["mandats"]), None))
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=period_start,
        period_end=period_end,
        status=OccupancyStatus(mandate) if mandate is not None else None,
    )
    if occupancy is None:
        return
    occupancy.add(
        "constituency",
        join_text(*taxonomies["circonscriptions"], *taxonomies["provinces"], sep=", "),
    )
    occupancy.add("politicalGroup", political_groups)
    context.emit(occupancy)
    context.emit(person)

    context.audit_data(record, ignore=IGNORE)


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
        data = context.fetch_json(
            context.data_url,
            params={"per_page": PER_PAGE, "_embed": 1, "offset": offset},
        )
        for record in data:
            crawl_member(
                context, position, categorisation, period_start, period_end, record
            )
        if len(data) < PER_PAGE:
            break
