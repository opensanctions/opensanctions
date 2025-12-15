from datetime import datetime
from lxml.html import HtmlElement
from normality import squash_spaces
from typing import NamedTuple

from zavod import Context, helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise, EXTENDED_AFTER_OFFICE_YEARS


class LegislatureTerm(NamedTuple):
    url: str
    text: str


POSITION_TOPICS = ["gov.legislative", "gov.national"]
CUTOFF_YEAR = datetime.now().year - get_after_office(POSITION_TOPICS)


def get_terms(context: Context, term: str) -> tuple[int | None, int | None]:
    res = context.lookup("term_years", term, warn_unmatched=True)
    if res:
        return res.start_date, res.end_date
    else:
        return None, None


def crawl_persons(
    context: Context, row: HtmlElement, start_date: int, end_date: int | None
) -> None:
    cells = h.xpath_elements(row, ".//td[@class='td1' or @class='td0']")
    name = h.xpath_strings(cells[0], ".//b/text()", expect_exactly=1)[0].strip()
    profile_url = h.xpath_strings(cells[0], ".//a/@href", expect_exactly=1)[0]

    group_texts = h.xpath_strings(cells[1], ".//a/text()")
    political_group = group_texts[0].strip() if group_texts else ""
    # Normalize whitespace in the name (different terms may include inconsistent spacing)
    name = squash_spaces(name)

    entity = context.make("Person")
    # Use the normalized name as the stable identifier. We don't have consistent unique identifiers
    # (like national IDs or member numbers) across all legislature terms, so the normalized name
    # is our only reliable way to link the same individual across multiple terms and avoid duplicates
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("political", political_group)
    entity.add("sourceUrl", profile_url)
    entity.add("citizenship", "be")

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
        start_date=str(start_date),
        end_date=str(end_date) if end_date else None,
        categorisation=categorisation,
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
    # Extract legislature terms from menu
    terms: list[LegislatureTerm] = []
    seen_urls: set[str] = set()
    for link in doc.findall('.//div[@class="menu"]//a'):
        url = link.get("href")
        text = h.element_text(link).strip()
        assert url is not None, url

        # Only keep entries with explicit date format "XX (YYYY-YYYY)"
        # The first link "Actuels" (current members) duplicates the most recent term,
        # so we filter for entries with parentheses to get unique dated terms.
        if text and "(" in text and ")" in text and url not in seen_urls:
            seen_urls.add(url)
            terms.append(LegislatureTerm(url=url, text=text))
    # Process each legislature term
    for idx, term in enumerate(terms):
        start_date, end_date = get_terms(context, term.text)
        assert start_date is not None, start_date
        # First term in the list is always the current legislature.
        # For current terms, we set end_date to None because members are still serving
        # and haven't left office yet. The end year in the lookup (e.g., "2025" for
        # "56 (2024-2025)") shouldn't be used as an actual end_date for occupancy records
        # until the term actually concludes and new elections occur.
        if idx == 0:
            end_date = None

        # Skip terms that ended before our cutoff year (skip this check for current term)
        if end_date is not None and end_date < CUTOFF_YEAR:
            context.log.info(f"Skipping old term {term.text} (ended {end_date})")
            continue

        # Fetch the member list page for this term
        term_doc = zyte_api.fetch_html(
            context,
            term.url,
            unblock_validator=unblock_validator,
            absolute_links=True,
            cache_days=4,
        )
        # Extract and process all members from the table
        table = h.xpath_elements(term_doc, "//table[@width='100%']", expect_exactly=1)[
            0
        ]
        for row in h.xpath_elements(table, ".//tr[td[@class='td1' or @class='td0']]"):
            crawl_persons(context, row, start_date, end_date)
