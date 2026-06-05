import re

from normality import squash_spaces

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

# The roster page id (the ".1435." segment of /NAME.1435.891.html) is a stable per-MP
# identifier, present only on the current convocation's table; archived convocations
# render plain text without links.
MP_ID_RE = re.compile(r"\.(\d+)\.\d+\.html")
DMY_RE = re.compile(r"\d{1,2}\.\d{1,2}\.\d{4}")
ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
# The date fragment ("1. avgusta 2022") inside a convocation label; the Serbian month
# name is translated via the dataset's `dates.months` mapping when parsed.
LABEL_DATE_RE = re.compile(r"\d{1,2}\.?\s+[^\s.]+\s+\d{4}")

POSITION_NAME = "Member of the National Assembly of Serbia"


def make_assembly_position(context: Context) -> Entity:
    return h.make_position(
        context,
        name=POSITION_NAME,
        country="rs",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295999",
    )


def parse_label_date(context: Context, label: str) -> str | None:
    """Parse the start date out of a convocation label like "Saziv od 1. avgusta 2022."."""
    match = LABEL_DATE_RE.search(label)
    if match is None:
        return None
    iso = h.extract_date(context.dataset, match.group(0))[0]
    return iso if ISO_DATE_RE.fullmatch(iso) else None


def cyrillic_name_index(doc: Element) -> dict[str, str]:
    """Map each MP id to its Cyrillic name from the Cyrillic mirror of the roster."""
    index: dict[str, str] = {}
    for link in h.xpath_elements(doc, "//table//td//a[@href]"):
        href = link.get("href") or ""
        match = MP_ID_RE.search(href)
        if match is not None:
            index[match.group(1)] = squash_spaces(h.element_text(link))
    return index


def member_tables(doc: Element) -> list[tuple[Element, bool]]:
    """Return the roster tables on a page, each flagged as the "departed members" table.

    A convocation page carries a table of members and (usually) a table of members who
    left before the term ended; they share the first two columns but differ in the
    third ("Mesto" vs "Trajanje mandata"). Other tables on the page are ignored.
    """
    tables = []
    for table in h.xpath_elements(doc, "//table"):
        header = h.xpath_elements(table, ".//tr[1]/td | .//tr[1]/th")
        labels = [h.element_text(c) for c in header]
        if len(labels) < 4 or not labels[0].startswith("Ime i prezime"):
            continue
        if labels[2] == "Mesto":
            tables.append((table, False))
        elif labels[2].startswith("Trajanje"):
            tables.append((table, True))
    return tables


def crawl_row(
    context: Context,
    row: Element,
    departed: bool,
    position: Entity,
    categorisation: PositionCategorisation,
    period_start: str | None,
    period_end: str | None,
    is_current: bool,
    cyrillic: dict[str, str],
) -> None:
    cells = h.xpath_elements(row, "./td")
    if len(cells) < 4:
        return
    # Names are stored verbatim, surname-first and as-cased by the source (e.g.
    # "ABRAMOVIĆ NENAD", "ALBIJANIĆ prof. dr MILOLJUB"). They also include academic
    # titles, digraph casing and parenthesised minority-language forms, so we don't try
    # to split or normalise them here. TODO: revisit with the rigour names framework to
    # split given/family names and lift out titles once that is wired up for datasets.
    name = squash_spaces(h.element_text(cells[0]))
    if not name:
        return
    party = squash_spaces(h.element_text(cells[1]))
    year_of_birth = squash_spaces(h.element_text(cells[3]))

    person = context.make("Person")
    links = h.xpath_strings(cells[0], ".//a/@href")
    mp_id = MP_ID_RE.search(links[0]) if links else None
    if mp_id is not None:
        person.id = context.make_slug("person", mp_id.group(1))
    else:
        # Archived convocations have no per-MP id; key on name + year of birth and rely
        # on downstream resolution to merge with the same person from other sources.
        person.id = context.make_id(name, year_of_birth)

    person.add("name", name, lang="srp")
    if mp_id is not None:
        person.add("name", cyrillic.get(mp_id.group(1)), lang="srp")
    h.apply_date(person, "birthDate", year_of_birth)
    person.add("political", party, lang="srp")
    # Serbian citizenship is required to stand for the National Assembly: Constitution
    # of Serbia (2006), Art. 52. https://www.constituteproject.org/constitution/Serbia_2006
    person.add("citizenship", "rs")

    if departed:
        # Third column is this member's own mandate range, e.g. "27.11.2024 - 17.04.2025.".
        dates = DMY_RE.findall(h.element_text(cells[2]))
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            start_date=dates[0] if dates else None,
            end_date=dates[1] if len(dates) > 1 else None,
        )
    else:
        # Full-term members are dated to the convocation period. We let make_occupancy
        # derive the status from the dates: for the current convocation no_end_implies_
        # current keeps them `current`; past convocations have a period_end and resolve
        # to `ended`.
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            period_start=period_start,
            period_end=period_end,
            no_end_implies_current=is_current,
        )
    if occupancy is None:
        return
    if links:
        occupancy.add("sourceUrl", links[0])
    context.emit(occupancy)
    context.emit(person)


def crawl_convocation(
    context: Context,
    doc: Element,
    source: str,
    position: Entity,
    categorisation: PositionCategorisation,
    period_start: str | None,
    period_end: str | None,
    is_current: bool,
    cyrillic: dict[str, str],
) -> None:
    tables = member_tables(doc)
    if not tables:
        # The two oldest convocations (2004, 2007) use a different column layout with no
        # birth-year column. We intentionally skip them rather than parse a lower-quality
        # variant; log it (info, not warning — this is a deliberate gap, not something
        # the crawler team can fix) so it stays visible and a future relapse is noticed.
        context.log.info("No recognised member table; skipping convocation", url=source)
        return
    for table, departed in tables:
        for row in h.xpath_elements(table, ".//tr")[1:]:
            crawl_row(
                context,
                row,
                departed,
                position,
                categorisation,
                period_start,
                period_end,
                is_current,
                cyrillic,
            )


def earliest_mandate_start(context: Context, doc: Element) -> str | None:
    """Return the earliest mandate start date on a convocation page, or None.

    The source never labels the current convocation with its start date, but its
    departed-members table records each mandate's start; the earliest of those is the
    date the convocation convened (its original members started then). We use it as the
    period end of the most recent archived convocation rather than hard-coding a date.
    """
    starts: list[str] = []
    for table, departed in member_tables(doc):
        if not departed:
            continue
        for row in h.xpath_elements(table, ".//tr")[1:]:
            cells = h.xpath_elements(row, "./td")
            if len(cells) >= 3:
                dates = DMY_RE.findall(h.element_text(cells[2]))
                if dates:
                    iso = h.extract_date(context.dataset, dates[0])[0]
                    if ISO_DATE_RE.fullmatch(iso):
                        starts.append(iso)
    return min(starts) if starts else None


def list_convocations(context: Context, doc: Element) -> list[tuple[str, str]]:
    """Return archived convocations as ``(url, start_date)``, newest first.

    The archive navigation (present on any archive page) links to every past
    convocation roster, labelled with its start date.
    """
    convocations: list[tuple[str, str]] = []
    seen: set[str] = set()
    for link in h.xpath_elements(doc, "//a[@href]"):
        href = link.get("href") or ""
        if "arhiva-saziva/saziv-od-" not in href or not href.endswith(".html"):
            continue
        if href in seen:
            continue
        start = parse_label_date(context, h.element_text(link))
        if start is None:
            continue
        seen.add(href)
        convocations.append((href, start))
    convocations.sort(key=lambda c: c[1], reverse=True)
    return convocations


def crawl(context: Context) -> None:
    cutoff = h.earliest_term_start(["gov.national"])
    position = make_assembly_position(context)
    context.emit(position)
    categorisation = categorise(context, position, default_is_pep=True)

    current = context.fetch_html(context.data_url, absolute_links=True, cache_days=1)

    # Current convocation also exists as a Cyrillic mirror (linked from the "ЋИР"
    # script toggle); join it by MP id to keep the Cyrillic spelling of each name.
    cyrillic: dict[str, str] = {}
    toggle = h.xpath_strings(current, '//a[contains(., "ЋИР")]/@href')
    if toggle:
        cyr_doc = context.fetch_html(toggle[0], absolute_links=True, cache_days=1)
        cyrillic = cyrillic_name_index(cyr_doc)

    # The current convocation isn't labelled with a start date; derive it from its own
    # mandate records so it can close out the most recent archived convocation's period.
    current_start = earliest_mandate_start(context, current)
    crawl_convocation(
        context,
        current,
        context.data_url,
        position,
        categorisation,
        current_start,
        None,
        True,
        cyrillic,
    )

    archive_links = h.xpath_strings(
        current, '//a[contains(@href, "arhiva-saziva/saziv-od-")]/@href'
    )
    assert archive_links, "No archive link found on the current roster"
    archive = context.fetch_html(archive_links[0], absolute_links=True, cache_days=7)
    convocations = list_convocations(context, archive)
    assert len(convocations) > 1, convocations

    for index, (url, start) in enumerate(convocations):
        if start < cutoff:
            continue
        # A convocation ended when the next (newer) one began: the previous entry in the
        # newest-first list, or the current convocation's start for the most recent one.
        period_end = convocations[index - 1][1] if index > 0 else current_start
        doc = context.fetch_html(url, absolute_links=True, cache_days=7)
        crawl_convocation(
            context,
            doc,
            url,
            position,
            categorisation,
            start,
            period_end,
            False,
            cyrillic,
        )
