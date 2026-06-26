import re
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.extract import zyte_api
from zavod.util import Element

# The site only resolves through Zyte with a browser render from an EU location.
GEO = "DE"
# A delegate who left mid-term is annotated in the list as "... * do 10.10.2024"
# ("do" = "until"); the date is their occupancy end date.
DO_RE = re.compile(r"\bdo\s+(\d{1,2}\.\d{1,2}\.\d{4})")
PAGE_RE = re.compile(r"[?&]page=(\d+)")
# "Date and place of birth" comes in several shapes, with the place (optional)
# trailing the date after a "." or "," separator:
#   numeric  — "20. 1. 1984.", "31. 5. 1974, Doboj", "03.01.1976. Kakanj"
#   English  — "31 January 1984, Bihać"
# Each pattern yields (date, place); the date feeds apply_date (formats in the YAML).
DOB_NUM_RE = re.compile(r"(\d{1,2}\.\s*\d{1,2}\.\s*\d{4})\.?\s*(.*)")
DOB_EN_RE = re.compile(r"(\d{1,2}\s+[A-Za-z]+\s+\d{4})\s*,?\s*(.*)")


def detail_field(doc: Element, label: str) -> str | None:
    """Return the value cell for a detail-page label.

    The member detail table mixes two row shapes: some labels sit in a ``<th>``
    (House, Party, Caucuse, ...), others in the row's first ``<td>`` (Date and
    place of birth, ...). In both, the value is the last ``<td>``. Returns None
    when the field is absent or empty.
    """
    rows = h.xpath_elements(
        doc,
        f".//tr[th[normalize-space()={label!r}] or td[normalize-space()={label!r}]]",
    )
    for row in rows:
        values = h.xpath_elements(row, "./td")
        if not values:
            continue
        text = h.element_text(values[-1])
        if text and text != label:
            return text
    return None


def crawl_member(
    context: Context,
    id_prefix: str,
    detail_url: str,
    end_date: str | None,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    # Require the member data table, not just the page header: a bare ".//h1"
    # validator lets a half-rendered page through, which Zyte would then cache.
    doc = zyte_api.fetch_html(
        context,
        detail_url,
        unblock_validator=".//th[normalize-space()='Party']",
        geolocation=GEO,
        cache_days=14,
    )
    heading = h.element_text(h.xpath_element(doc, ".//h1"))  # "Surname, Given"
    last, _, first = heading.partition(",")

    member_id = detail_url.rstrip("/").rsplit("/", 1)[-1].split("?")[0]
    person = context.make("Person")
    person.id = context.make_slug(id_prefix, member_id)
    h.apply_name(person, first_name=first.strip(), last_name=last.strip())
    # BiH citizenship is a legal precondition to stand and serve: Election Law of BiH
    # Art. 1.4(1). https://www.izbori.ba/Documents/documents/English/Laws/BIHElectionlaw.pdf
    person.add("citizenship", "ba")
    person.add("political", detail_field(doc, "Party"))
    person.add("sourceUrl", detail_url)

    dob_place = detail_field(doc, "Date and place of birth")
    if dob_place is not None:
        num = DOB_NUM_RE.match(dob_place)
        eng = DOB_EN_RE.match(dob_place)
        if num is not None:
            # collapse "20. 1. 1984" -> "20.1.1984" so one numeric format matches
            date, place = num.group(1).replace(" ", ""), num.group(2)
        elif eng is not None:
            date, place = eng.group(1), eng.group(2)  # "31 January 1984"
        else:
            context.log.warning(
                "Unparsed date of birth", value=dob_place, url=detail_url
            )
            date, place = None, None
        h.apply_date(person, "birthDate", date)
        person.add("birthPlace", (place or "").lstrip(",.").strip() or None)

    occupancy = h.make_occupancy(
        context, person, position, end_date=end_date, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("politicalGroup", detail_field(doc, "Caucuse"))  # in-chamber faction
    occupancy.add("constituency", detail_field(doc, "Election unit / Entity"))
    context.emit(occupancy)
    context.emit(person)


def fetch_list_page(
    context: Context, base_url: str, list_path: str, page: int, detail_marker: str
) -> Element:
    return zyte_api.fetch_html(
        context,
        urljoin(base_url, f"{list_path}?page={page}&lang=en"),
        unblock_validator=f".//a[contains(@href, {detail_marker!r})]",
        absolute_links=True,
        geolocation=GEO,
        cache_days=1,
    )


def last_page_number(doc: Element) -> int:
    """Return the highest ``?page=N`` linked from a list page (1 if unpaginated)."""
    numbers = [
        int(m.group(1))
        for href in h.xpath_strings(doc, ".//a/@href")
        for m in [PAGE_RE.search(href)]
        if m is not None
    ]
    return max(numbers) if numbers else 1


def crawl_list_page(
    context: Context,
    doc: Element,
    id_prefix: str,
    detail_marker: str,
    position: Entity,
    categorisation: PositionCategorisation,
    seen: set[str],
) -> None:
    for link in h.xpath_elements(doc, f".//a[contains(@href, {detail_marker!r})]"):
        href = link.get("href")
        assert isinstance(href, str), href
        # List links are bare (no ?lang=en); without it the detail page renders
        # in Bosnian and the English field labels ("Party", ...) are absent. The
        # same link appears twice per row (photo + name), so dedup by URL.
        detail_url = urljoin(href.strip(), "?lang=en")
        if detail_url in seen:
            continue
        seen.add(detail_url)
        # The "* do <date>" marker may sit on either link, so search the whole row.
        rows = h.xpath_elements(link, "ancestor::tr[1]")
        match = DO_RE.search(h.element_text(rows[0])) if rows else None
        end_date = match.group(1) if match is not None else None
        crawl_member(context, id_prefix, detail_url, end_date, position, categorisation)


def crawl_chamber(
    context: Context,
    base_url: str,
    id_prefix: str,
    list_path: str,
    detail_marker: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    seen: set[str] = set()
    first = fetch_list_page(context, base_url, list_path, 1, detail_marker)
    crawl_list_page(
        context, first, id_prefix, detail_marker, position, categorisation, seen
    )
    for page in range(2, last_page_number(first) + 1):
        doc = fetch_list_page(context, base_url, list_path, page, detail_marker)
        crawl_list_page(
            context, doc, id_prefix, detail_marker, position, categorisation, seen
        )


def crawl(context: Context) -> None:
    hor = h.make_position(
        context,
        name="Member of the House of Representatives of Bosnia and Herzegovina",
        country="ba",
        wikidata_id="Q21290855",
    )
    hop = h.make_position(
        context,
        name="Member of the House of Peoples of Bosnia and Herzegovina",
        country="ba",
        wikidata_id="Q21328613",
    )
    hor_cat = categorise(context, hor, default_is_pep=True)
    hop_cat = categorise(context, hop, default_is_pep=True)
    context.emit(hor)
    context.emit(hop)

    base_url = context.dataset.url
    assert base_url is not None, "dataset url is required as the base URL"
    crawl_chamber(
        context,
        base_url,
        "representative",
        "/representative/list",
        "/representative/detail/",
        hor,
        hor_cat,
    )
    crawl_chamber(
        context,
        base_url,
        "delegate",
        "/delegate/list",
        "/delegate/detail/",
        hop,
        hop_cat,
    )
