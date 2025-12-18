from datetime import datetime
from lxml.html import HtmlElement

from zavod import Context, helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise, get_after_office, OccupancyStatus


POSITION_TOPICS = ["gov.legislative", "gov.national"]
CUTOFF_DATE = (datetime.now() - get_after_office(POSITION_TOPICS)).year


def get_terms(context: Context, term: str) -> tuple[int | None, int | None]:
    res = context.lookup("term_years", term, warn_unmatched=True)
    if res:
        return res.start_date, res.end_date
    else:
        return None, None


def get_dob(context: Context, profile_url: str) -> str:
    pep_doc = zyte_api.fetch_html(
        context,
        profile_url,
        unblock_validator="//table",
        absolute_links=True,
        cache_days=4,
    )
    bio_text = h.xpath_strings(
        pep_doc,
        './/td/p[contains(., "| Né")]/text()',
        expect_exactly=1,
    )[0]
    # Split on | and find the part with "Né"
    parts = [part.strip() for part in bio_text.split("|")]
    birth_info = next(part for part in parts if part.startswith("Né"))
    date_str = birth_info.split(" le ")[-1].rstrip(".")
    return date_str


def crawl_persons(
    context: Context,
    row: HtmlElement,
    status: OccupancyStatus,
) -> None:
    cells = h.xpath_elements(row, ".//td[@class='td1' or @class='td0']")
    name = h.xpath_strings(cells[0], ".//b/text()", expect_exactly=1)[0].strip()
    profile_url = h.xpath_strings(cells[0], ".//a/@href", expect_exactly=1)[0]

    group_texts = h.xpath_strings(cells[1], ".//a/text()")
    political_group = group_texts[0].strip() if group_texts else ""
    dob = get_dob(context, profile_url)

    entity = context.make("Person")
    entity.id = context.make_id(name, dob)
    entity.add("name", name)
    entity.add("political", political_group)
    entity.add("sourceUrl", profile_url)
    entity.add("citizenship", "be")
    h.apply_date(entity, "birthDate", dob)

    position = h.make_position(
        context,
        name="Member of the Chamber of Representatives of Belgium",
        wikidata_id="Q15705021",
        country="be",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person=entity,
        position=position,
        categorisation=categorisation,
        status=status,
    )
    if occupancy is not None:
        context.emit(entity)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context) -> None:
    unblock_validator = "//table[@width='100%']"
    # Fetch the main page with all legislature terms
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=unblock_validator,
        absolute_links=True,
        cache_days=4,
    )
    # Extract and process legislature terms from menu
    for idx, link in enumerate(doc.findall('.//div[@class="menu"]//a')):
        url = link.get("href")
        text = h.element_text(link).strip()
        assert url is not None, url

        if not text:
            continue

        # Legislative term dates (e.g., "55 (2019-2024)") don't reflect individual members'
        # actual service periods. Members may join/leave mid-term. We use explicit status
        # (CURRENT/ENDED) instead of dates to avoid misrepresenting when someone held office.
        # Term dates are only used to filter out old legislatures before the cutoff.
        is_current = idx == 0
        status = OccupancyStatus.CURRENT if is_current else OccupancyStatus.ENDED

        # For historical terms, check if they ended before cutoff
        if not is_current and "(" in text and ")" in text:
            _, end_date = get_terms(context, text)
            if end_date is not None and end_date < CUTOFF_DATE:
                context.log.info(f"Skipping old term {text} (ended {end_date})")
                continue

        context.log.info(f"Processing term: {text} (status={status})")
        # Fetch the member list page for this term
        term_doc = zyte_api.fetch_html(
            context,
            url,
            unblock_validator=unblock_validator,
            absolute_links=True,
            cache_days=4,
        )
        # Extract and process all members from the table
        table = h.xpath_elements(term_doc, "//table[@width='100%']", expect_exactly=1)[
            0
        ]
        for row in h.xpath_elements(table, ".//tr[td[@class='td1' or @class='td0']]"):
            crawl_persons(context, row, status)
