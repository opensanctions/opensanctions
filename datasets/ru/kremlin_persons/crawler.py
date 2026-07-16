import re

from lxml import etree

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

BASE_URL = "http://en.kremlin.ru"


def apply_birth_details(
    context: Context, person: Entity, doc: etree._Element, url: str
) -> None:
    """Apply birth date/place and, for the deceased, death date from the biography.

    These details are only ever available as free-text prose, so rather than
    parse them in code we route each raw line through the ``details`` lookup,
    which maps an exact line to ISO date(s) and cleaned place name(s). A birth
    line is expected for everyone (missing/ambiguous lines are warned about); a
    death line only appears for the deceased. Unmatched lines are warned about
    and left unset, never guessed at.
    """
    lines = [
        h.element_text(el)
        for el in h.xpath_elements(doc, ".//dl[@class='separate_dates']//dd")
    ]

    # "born" also appears in unrelated lines (e.g. "...son, Ilya (born 1995)"),
    # so only take a leading "Born ..." or a subject's "... was born ..." line.
    born_lines = [
        line
        for line in lines
        if line.lower().startswith("born") or "was born" in line.lower()
    ]
    if len(born_lines) == 0:
        context.log.warning("No birth line found in biography", url=url)
    elif len(born_lines) > 1:
        context.log.warning("Multiple birth lines found in biography", url=url)
    else:
        result = context.lookup("details", born_lines[0], warn_unmatched=True)
        if result is not None:
            h.apply_date(person, "birthDate", result.birth_date)
            person.add("birthPlace", result.birth_place)

    # "died" can also refer to a relative, so only take a leading "Died ..." or a
    # subject's "... died on ..." line.
    died_lines = [
        line
        for line in lines
        if line.lower().startswith("died") or "died on" in line.lower()
    ]
    if len(died_lines) > 1:
        context.log.warning("Multiple death lines found in biography", url=url)
    elif len(died_lines) == 1:
        result = context.lookup("details", died_lines[0], warn_unmatched=True)
        if result is not None:
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

    apply_birth_details(context, person, doc, url)
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
