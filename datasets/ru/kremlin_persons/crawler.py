import re

from lxml import etree

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

BASE_URL = "http://en.kremlin.ru"
# Membership listings of Russian state bodies chaired by / attached to the
# President. Each page lists a body's members with their current role; the roles
# are all treated as PEP positions (see crawl_members).
MEMBER_URLS = [
    "http://en.special.kremlin.ru/structure/state-council/members",
    "http://en.special.kremlin.ru/structure/security-council/members",
    "http://en.special.kremlin.ru/structure/administration/members",
]


# The biographical detail we extract, one entry per kind of line. Each line is
# matched by a leading verb ("Born"/"Died") or an inline phrase ("... was born
# ...", "... died on ..."), which avoids unrelated mentions such as a child's
# "(born 1995)" or a relative's death. A birth line is expected for everyone, so
# its absence is warned about; a death line only appears for the deceased.
DETAILS = [
    ("birth", "born", "was born", True),
    ("death", "died", "died on", False),
]


def apply_details(
    context: Context, person: Entity, doc: etree._Element, url: str
) -> None:
    """Apply birth date/place and, for the deceased, death date from the biography.

    These details are only ever available as free-text prose, so rather than
    parse them in code we route each matched line through the ``details`` lookup,
    which maps an exact line to ISO date(s) and cleaned place name(s). Unmatched
    lines are warned about and left unset, never guessed at.
    """
    lines = [
        h.element_text(el)
        for el in h.xpath_elements(doc, ".//dl[@class='separate_dates']//dd")
    ]

    for kind, verb, phrase, warn_missing in DETAILS:
        matched = [
            line
            for line in lines
            if line.lower().startswith(verb) or phrase in line.lower()
        ]
        if len(matched) == 0:
            if warn_missing:
                context.log.warning(f"No {kind} line found in biography", url=url)
            continue
        if len(matched) > 1:
            context.log.warning(f"Multiple {kind} lines found in biography", url=url)
            continue
        result = context.lookup("details", matched[0], warn_unmatched=True)
        if result is None:
            continue
        # A line only carries the fields for its kind; the rest are None no-ops.
        h.apply_date(person, "birthDate", result.birth_date)
        person.add("birthPlace", result.birth_place)
        h.apply_date(person, "deathDate", result.death_date)


def apply_biography(person: Entity, doc: etree._Element) -> None:
    """Store the biography's dated timeline verbatim in ``notes``.

    The biography is a ``<dl class='separate_dates'>`` of ``<dt>`` period /
    ``<dd>`` event pairs (a few ``<dd>`` entries, such as the birth line, stand
    alone). It is rendered as one "period: event" line per entry, preserving the
    source text.
    """
    timeline = h.xpath_element(doc, ".//dl[@class='separate_dates']")
    lines: list[str] = []
    period: str | None = None
    for child in timeline.iterchildren():
        if child.tag == "dt":
            period = h.element_text(child) or None
        elif child.tag == "dd":
            text = h.element_text(child)
            lines.append(f"{period}: {text}" if period is not None else text)
            period = None
    if len(lines) > 0:
        person.add("notes", "\n".join(lines))


def crawl_position(
    context: Context, person: Entity, doc: etree._Element, url: str
) -> None:
    """Emit the person's most recently listed Russian state position, if any.

    People with a biography but no single unambiguous title are still recorded
    (as persons of interest by the caller); only one non-foreign title produces
    a Position and PEP Occupancy.
    """
    title_element = h.xpath_elements(
        doc, ".//div[@class='persona__info']//div[@itemprop='jobTitle']"
    )
    if len(title_element) == 0:
        return
    position_title = h.element_text(title_element[0])
    # Only Russian-state figures get a narrative biography here; foreign leaders
    # get an events-only page and are never crawled.
    position = h.make_position(
        context,
        name=position_title,
        country="ru",
    )
    categorisation = categorise(context, position, default_is_pep=None)
    if categorisation.is_pep is not True:
        return

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is not None:
        context.emit(position)
        context.emit(occupancy)


def crawl_members(context: Context, url: str) -> None:
    """Emit each listed member of a Kremlin state body as a current PEP.

    These structure pages list a body's members as ``contacts_name`` cards, each
    pairing a link to the person's catalogue entry with their current role. We
    only read the listing (not the linked detail pages), reusing the catalogue
    id so members merge with the biography-crawled persons. Every listed role is
    treated as a currently-held PEP position.
    """
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=".//p[@class='contacts_name']",
        html_source="httpResponseBody",
        cache_days=1,
    )
    id_pattern = re.compile(r"/catalog/persons/(\d+)/")
    for card in h.xpath_elements(doc, ".//p[@class='contacts_name']"):
        link = h.xpath_element(card, "./a[contains(@href, '/catalog/persons/')]")
        match = id_pattern.search(link.get("href", ""))
        if match is None:
            context.log.warning("Member link without a person id", url=url)
            continue
        person_id = match.group(1)
        family_name = h.element_text(
            h.xpath_element(card, ".//*[@itemprop='familyName']")
        )
        given_name = h.element_text(
            h.xpath_element(card, ".//*[@itemprop='givenName']")
        )

        person = context.make("Person")
        person.id = context.make_slug(person_id)
        h.apply_name(person, given_name=given_name, last_name=family_name, lang="eng")
        person.add("country", "ru")
        person.add("sourceUrl", url)
        person.add("topics", "poi")

        # The role sits in a sibling ``jobTitle`` within the same card block.
        block = card.getparent()
        role_elements = (
            h.xpath_elements(block, ".//*[@itemprop='jobTitle']")
            if block is not None
            else []
        )
        if len(role_elements) != 1:
            context.log.warning(
                "Member without a single role title",
                url=url,
                person=person_id,
                roles=len(role_elements),
            )
            context.emit(person)
            continue

        position = h.make_position(
            context,
            name=h.element_text(role_elements[0]),
            country="ru",
        )
        # Membership of these presidential bodies is itself the PEP signal, so
        # every listed role is classified as a PEP position by default.
        categorisation = categorise(context, position, default_is_pep=True)
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is not None:
            context.emit(position)
            context.emit(occupancy)
        context.emit(person)


def crawl_person(context: Context, person_id: str) -> None:
    url = f"{BASE_URL}/catalog/persons/{person_id}/biography"
    name_xpath = ".//*[@itemprop='familyName']"
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=name_xpath,
        html_source="httpResponseBody",
        cache_days=7,
    )

    family_name = h.element_text(h.xpath_element(doc, name_xpath))
    given_name = h.element_text(h.xpath_element(doc, ".//*[@itemprop='givenName']"))

    person = context.make("Person")
    person.id = context.make_slug(person_id)
    h.apply_name(person, given_name=given_name, last_name=family_name, lang="eng")
    # Positions here are Presidential Executive Office appointees, civil servants and
    # security service officials, not directly elected, so `country` rather than
    # `citizenship` (see zavod/docs/peps.md, "Properties to capture").
    person.add("country", "ru")
    person.add("sourceUrl", url)
    # A Kremlin biography alone makes someone notable enough to record, even with
    # no current state position; mark every such person as a person of interest.
    # Those with a listed position additionally gain the PEP role via the
    # occupancy created in crawl_position.
    person.add("topics", "poi")

    apply_details(context, person, doc, url)
    apply_biography(person, doc)
    crawl_position(context, person, doc, url)

    context.emit(person)


def crawl(context: Context) -> None:
    # Plain requests get connection-timed-out/blocked by this site, even for the
    # listing page, so route through Zyte like the per-person pages below.
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=".//a[contains(@href, '/catalog/persons/')]",
        html_source="httpResponseBody",
        cache_days=1,
    )
    # Only crawl people the Kremlin has written a narrative biography for: the
    # directory links them via `/biography`, while everyone else (foreign leaders,
    # people who merely appear in event listings) gets an `/events` link with no
    # date/place of birth. Biography pages are both the reliable Russian-official
    # signal and the only pages carrying the biographical detail we want.
    link_pattern = re.compile(r"/catalog/persons/(\d+)/biography")

    person_ids: set[str] = set()
    for link in h.xpath_elements(doc, ".//a[contains(@href, '/catalog/persons/')]"):
        href = link.get("href", "")
        match = link_pattern.search(href)
        if match is None:
            continue
        person_ids.add(match.group(1))

    for person_id in person_ids:
        crawl_person(context, person_id)

    for url in MEMBER_URLS:
        crawl_members(context, url)
