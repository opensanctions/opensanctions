from datetime import datetime
from lxml.html import HtmlElement

from zavod import Context, helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise, get_after_office, OccupancyStatus


POSITION_TOPICS = ["gov.legislative", "gov.national"]
CUTOFF_DATE = (datetime.now() - get_after_office(POSITION_TOPICS)).year


def get_term_dates(context: Context, term: str) -> tuple[int | None, int | None]:
    res = context.lookup("term_years", term, warn_unmatched=True)
    if res:
        return res.start_date, res.end_date
    else:
        return None, None


def get_dob(context: Context, profile_url: str) -> str | None:
    pep_doc = zyte_api.fetch_html(
        context,
        profile_url,
        unblock_validator="//table",
        absolute_links=True,
        cache_days=4,
    )
    bio_text_list = h.xpath_strings(
        pep_doc, './/td/p[contains(., "| Né") or contains(., "| Ne")]/text()'
    )
    if bio_text_list:
        bio_text = bio_text_list[0]
    else:
        context.log.warn(f"Could not find bio text for {profile_url}")
        return None
    # Split on | and find the part with "Né" or "Ne"
    parts = [part.strip() for part in bio_text.split("|")]
    birth_info = next(part for part in parts if part.startswith((("Né", "Ne"))))
    date_str = birth_info.split(" le ")[-1].rstrip(".")
    return date_str


def crawl_persons(
    context: Context,
    row: HtmlElement,
    status: OccupancyStatus,
) -> None:
    cells = h.xpath_elements(row, ".//td[@class='td1' or @class='td0']")
    name = h.xpath_string(cells[0], ".//b/text()")
    profile_url = h.xpath_string(cells[0], ".//a/@href")

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
    legislature_menu = h.xpath_element(
        doc, './/b[text() = "Actuels"]/ancestor::div[@class="menu"]'
    )

    links = legislature_menu.findall(".//a")
    # The menu lists the current term twice: once under "Actuels" and once
    # as a dated term (e.g. "56 (2024-2025)"). Confirm and strip the duplicate.
    if links[0].get("href")[:-1] != links[1].get("href")[:-1]:
        context.log.warning(
            "Legislature menu structure has changed",
            urls=[l.get("href") for l in links[:2]],
        )
        return
    current_url, links = links[0].get("href"), links[1:]

    for link in links:
        # Legislative term dates (e.g., "55 (2019-2024)") don't reflect individual members'
        # actual service periods. Members may join/leave mid-term. We use explicit status
        # (CURRENT/ENDED) instead of dates to avoid misrepresenting when someone held office.
        # Term dates are only used to filter out old legislatures before the cutoff.
        text = h.element_text(link).strip()
        _, end_date = get_term_dates(context, text)
        if end_date is not None and end_date < CUTOFF_DATE:
            context.log.info(f"Skipping old term: {text}")
            continue
        url = link.get("href")
        status = (
            OccupancyStatus.CURRENT if url == current_url else OccupancyStatus.ENDED
        )
        assert status is not None, f"Could not determine status for term {text}"

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
        table = h.xpath_element(term_doc, "//table[@width='100%']")
        for row in h.xpath_elements(table, ".//tr[td[@class='td1' or @class='td0']]"):
            crawl_persons(context, row, status)
