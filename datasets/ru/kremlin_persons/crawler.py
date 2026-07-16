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


def fetch(
    context: Context, url: str, validator: str, cache_days: int
) -> etree._Element:
    """Fetch a Kremlin page via Zyte; plain requests are blocked by the site."""
    return zyte_api.fetch_html(
        context,
        url,
        unblock_validator=validator,
        html_source="httpResponseBody",
        cache_days=cache_days,
    )


def make_person(
    context: Context,
    person_id: str,
    given_name: str,
    family_name: str,
    source_url: str,
) -> Entity:
    """Build a person-of-interest from a Kremlin catalogue id and name.

    The id is reused as the entity slug so the same person is merged across the
    person directory and the membership listings.
    """
    person = context.make("Person")
    person.id = context.make_slug(person_id)
    h.apply_name(person, given_name=given_name, last_name=family_name, lang="eng")
    # These are Presidential appointees, civil servants and security officials,
    # not directly elected, so `country` rather than `citizenship` (see
    # zavod/docs/peps.md, "Properties to capture").
    person.add("country", "ru")
    person.add("sourceUrl", source_url)
    # A place in the Kremlin catalogue alone makes someone notable enough to
    # record, even without a current position; mark everyone a person of interest.
    person.add("topics", "poi")
    return person


def emit_position(
    context: Context, person: Entity, title: str, *, default_is_pep: bool | None
) -> None:
    """Emit a Position and a current PEP Occupancy held by ``person``.

    ``default_is_pep`` is the fallback classification for a position not yet
    reviewed; nothing is emitted unless the position resolves to a PEP.
    """
    position = h.make_position(context, name=title, country="ru")
    categorisation = categorise(context, position, default_is_pep=default_is_pep)
    if categorisation.is_pep is not True:
        return
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is not None:
        context.emit(position)
        context.emit(occupancy)


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


def crawl_members(context: Context, url: str) -> None:
    """Emit each listed member of a Kremlin state body as a current PEP.

    These structure pages list a body's members as ``contacts_name`` cards, each
    pairing a link to the person's catalogue entry with their current role. We
    only read the listing (not the linked detail pages). Every listed role is
    treated as a currently-held PEP position.
    """
    doc = fetch(context, url, ".//p[@class='contacts_name']", cache_days=1)
    id_pattern = re.compile(r"/catalog/persons/(\d+)/")
    for card in h.xpath_elements(doc, ".//p[@class='contacts_name']"):
        link = h.xpath_element(card, "./a[contains(@href, '/catalog/persons/')]")
        href = link.get("href", "")
        match = id_pattern.search(href)
        if match is None:
            context.log.warning("Member link without a person id", url=url)
            continue
        # Link to the member's own catalogue page (built against BASE_URL so it
        # matches the biography crawl's sourceUrl and merges), not the listing.
        person = make_person(
            context,
            match.group(1),
            h.element_text(h.xpath_element(card, ".//*[@itemprop='givenName']")),
            h.element_text(h.xpath_element(card, ".//*[@itemprop='familyName']")),
            f"{BASE_URL}{href}",
        )

        # The role sits in a sibling ``jobTitle`` within the same card block.
        block = card.getparent()
        roles = (
            h.xpath_elements(block, ".//*[@itemprop='jobTitle']")
            if block is not None
            else []
        )
        if len(roles) == 1:
            # Membership of these presidential bodies is itself the PEP signal,
            # so every listed role is classified as a PEP position by default.
            emit_position(
                context, person, h.element_text(roles[0]), default_is_pep=True
            )
        else:
            context.log.warning(
                "Member without a single role title",
                url=url,
                person=person.id,
                roles=len(roles),
            )
        context.emit(person)


def crawl_person(context: Context, person_id: str) -> None:
    url = f"{BASE_URL}/catalog/persons/{person_id}/biography"
    name_xpath = ".//*[@itemprop='familyName']"
    doc = fetch(context, url, name_xpath, cache_days=7)

    person = make_person(
        context,
        person_id,
        h.element_text(h.xpath_element(doc, ".//*[@itemprop='givenName']")),
        h.element_text(h.xpath_element(doc, name_xpath)),
        url,
    )

    apply_details(context, person, doc, url)
    apply_biography(person, doc)

    # Only Russian-state figures get a narrative biography here; foreign leaders
    # get an events-only page and are never crawled. A single listed title is
    # recorded as a review-gated position (default_is_pep=None).
    titles = h.xpath_elements(
        doc, ".//div[@class='persona__info']//div[@itemprop='jobTitle']"
    )
    if len(titles) == 1:
        emit_position(context, person, h.element_text(titles[0]), default_is_pep=None)

    context.emit(person)


def crawl(context: Context) -> None:
    # Plain requests get connection-timed-out/blocked by this site, even for the
    # listing page, so route through Zyte like the per-person pages below.
    doc = fetch(
        context,
        context.data_url,
        ".//a[contains(@href, '/catalog/persons/')]",
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
