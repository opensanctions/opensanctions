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
# are all treated as PEP positions (see crawl_pep).
MEMBER_URLS = [
    "http://en.special.kremlin.ru/structure/state-council/members",
    "http://en.special.kremlin.ru/structure/security-council/members",
    "http://en.special.kremlin.ru/structure/administration/members",
]
PERSON_ID_RE = re.compile(r"/catalog/persons/(\d+)/")


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


def emit_position(
    context: Context, person: Entity, title: str, *, default_is_pep: bool | None
) -> None:
    """Emit a Position and a current Occupancy held by ``person``, if it's a PEP.

    ``default_is_pep`` is the fallback classification for a position not yet
    reviewed: ``crawl_pep`` passes ``True`` because membership of a presidential
    body is itself the PEP signal, while ``crawl_associate`` passes ``None`` so a
    biography's job title stays review-gated. Nothing is emitted unless the
    position resolves to a PEP.
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


def crawl_associate(context: Context, person_id: str) -> None:
    """Emit one person from the Kremlin directory as an associate of the President.

    Fetches the narrative biography, applies birth/death details and the timeline,
    and records a single listed job title as a review-gated position.
    """
    url = f"{BASE_URL}/catalog/persons/{person_id}/biography"
    name_xpath = ".//*[@itemprop='familyName']"
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=name_xpath,
        html_source="httpResponseBody",
        cache_days=7,
    )

    given_name = h.element_text(h.xpath_element(doc, ".//*[@itemprop='givenName']"))
    family_name = h.element_text(h.xpath_element(doc, name_xpath))

    person = context.make("Person")
    person.id = context.make_slug(person_id)
    h.apply_name(person, given_name=given_name, last_name=family_name, lang="eng")
    # These are Presidential appointees, civil servants and security officials,
    # not directly elected, so `country` rather than `citizenship` (see
    # zavod/docs/peps.md, "Properties to capture").
    person.add("country", "ru")
    person.add("sourceUrl", url)
    # A Kremlin biography alone makes someone notable enough to record, even
    # without a current position; mark everyone a person of interest.
    person.add("topics", "poi")

    apply_details(context, person, doc, url)
    apply_biography(person, doc)

    # Only Russian-state figures get a narrative biography here; foreign leaders
    # get an events-only page and are never crawled. A single listed title is
    # recorded as a review-gated position.
    titles = h.xpath_elements(
        doc, ".//div[@class='persona__info']//div[@itemprop='jobTitle']"
    )
    if len(titles) == 1:
        emit_position(context, person, h.element_text(titles[0]), default_is_pep=None)

    context.emit(person)


def crawl_pep(context: Context, card: etree._Element, url: str) -> None:
    """Emit one member of a presidential state body as a current PEP.

    Reads a single ``contacts_name`` card from a membership listing (surface only,
    no detail-page fetch), reusing the catalogue id so the member merges with any
    biography-crawled associate. The listed role is a currently-held PEP position.
    """
    link = h.xpath_element(card, "./a[contains(@href, '/catalog/persons/')]")
    href = link.get("href", "")
    match = PERSON_ID_RE.search(href)
    if match is None:
        context.log.warning("Member link without a person id", url=url)
        return
    given_name = h.element_text(h.xpath_element(card, ".//*[@itemprop='givenName']"))
    family_name = h.element_text(h.xpath_element(card, ".//*[@itemprop='familyName']"))

    person = context.make("Person")
    person.id = context.make_slug(match.group(1))
    h.apply_name(person, given_name=given_name, last_name=family_name, lang="eng")
    person.add("country", "ru")
    # Link to the member's own catalogue page (built against BASE_URL so it matches
    # the biography crawl's sourceUrl and merges), not the listing.
    person.add("sourceUrl", f"{BASE_URL}{href}")
    person.add("topics", "poi")

    # The role sits in a sibling ``jobTitle`` within the same card block.
    block = card.getparent()
    roles = (
        h.xpath_elements(block, ".//*[@itemprop='jobTitle']")
        if block is not None
        else []
    )
    if len(roles) == 1:
        emit_position(context, person, h.element_text(roles[0]), default_is_pep=True)
    else:
        context.log.warning(
            "Member without a single role title",
            url=url,
            person=person.id,
            roles=len(roles),
        )

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
    # people who merely appear in event listings) gets an `/events` link we skip.
    link_pattern = re.compile(r"/catalog/persons/(\d+)/biography")

    person_ids: set[str] = set()
    for link in h.xpath_elements(doc, ".//a[contains(@href, '/catalog/persons/')]"):
        match = link_pattern.search(link.get("href", ""))
        if match is not None:
            person_ids.add(match.group(1))

    # An associate and a council/administration member can be the same person;
    # they merge by catalogue id, so an entity may gain both a biography and a
    # PEP occupancy.
    for person_id in person_ids:
        crawl_associate(context, person_id)

    for url in MEMBER_URLS:
        doc = zyte_api.fetch_html(
            context,
            url,
            unblock_validator=".//p[@class='contacts_name']",
            html_source="httpResponseBody",
            cache_days=1,
        )
        for card in h.xpath_elements(doc, ".//p[@class='contacts_name']"):
            crawl_pep(context, card, url)
