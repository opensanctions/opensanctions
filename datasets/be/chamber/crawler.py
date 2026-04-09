import itertools
from lxml.html import HtmlElement
from normality import squash_spaces

from zavod import Context, helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise


POSITION_TOPICS = ["gov.legislative", "gov.national"]


def get_term_dates(context: Context, term: str) -> tuple[int | None, int | None]:
    _, years = term.split("(", 1)
    years = years.rstrip(")")
    start, end = years.split("-", 1)
    assert len(start) == 4 and len(end) == 4
    int(start) != end  # just see if this raises an exception
    return start, end


def get_dob_bio(
    context: Context, profile_url: str
) -> tuple[str, str] | tuple[None, str]:
    """Extract the date of birth and bio text from a member's profile page.

    Returns a tuple of (date_of_birth, bio_text) where:
    - date_of_birth is a string like "22 mai 1963", or None if not found
    - bio_text is the raw biography string, kept even when no DOB is found
      so that we don't lose the information when DOB is embedded in a plain
      sentence without a pipe separator.
    """
    pep_doc = zyte_api.fetch_html(
        context,
        profile_url,
        unblock_validator="//table",
        absolute_links=True,
        cache_days=4,
    )
    # First try: pipe-separated bio format e.g. "... | Né le 22 mai 1963 | ..."
    pipe_texts = h.xpath_strings(
        pep_doc, './/td/p[contains(., "| Né") or contains(., "| Ne")]/text()'
    )
    if pipe_texts:
        parts = [part.strip() for part in pipe_texts[0].split("|")]
        birth_part = next(part for part in parts if part.startswith((("Né", "Ne"))))
        date_str = birth_part.split(" le ")[-1].rstrip(".")
        return date_str, squash_spaces(pipe_texts[0])
    # Fallback: DOB embedded in plain sentence e.g. "Née à Tournai le 22 mai 1963."
    plain_text = h.xpath_string(
        pep_doc,
        './/td/p[contains(., "Né") or contains(., "Ne") or contains(., "né")]/text()',
    )
    return None, squash_spaces(plain_text)


def crawl_person(
    context: Context,
    row: HtmlElement,
    period_start: str,
    period_end: str,
    is_current_term: bool,
) -> None:
    cells = h.xpath_elements(row, ".//td[@class='td1' or @class='td0']")
    name = h.xpath_string(cells[0], ".//b/text()")
    profile_url = h.xpath_string(cells[0], ".//a/@href")

    group_texts = h.xpath_strings(cells[1], ".//a/text()")
    political_group = group_texts[0].strip() if group_texts else ""
    dob, bio = get_dob_bio(context, profile_url)

    entity = context.make("Person")
    entity.id = context.make_id(name, dob)
    entity.add("name", name)
    entity.add("political", political_group)
    entity.add("sourceUrl", profile_url)
    entity.add("citizenship", "be")
    entity.add("notes", bio)
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
        no_end_implies_current=is_current_term,
        period_start=period_start,
        period_end=period_end,
    )
    if occupancy is not None:
        context.emit(entity)
        context.emit(position)
        context.emit(occupancy)


def crawl_term(
    context,
    link: HtmlElement,
    unblock_validator: str,
) -> None:
    # Term dates are only used to skip legislatures outside our coverage window.
    # We don't use them to set occupancy dates — members may join/leave mid-term.
    text = h.element_text(link).strip()
    if text == "Actuels":
        period_start, period_end = None, None
        is_current_term = True
    else:
        period_start, period_end = get_term_dates(context, text)
        is_current_term = False

    if not is_current_term and period_start < h.earliest_period_start(POSITION_TOPICS):
        context.log.info(
            f"Skipping term {text} with start date {period_start} outside coverage window"
        )
        return

    url = link.attrib["href"]
    assert url is not None, f"Term link missing URL for term {text}"

    # Fetch the member list page for this term
    term_doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=unblock_validator,
        absolute_links=True,
        cache_days=4,
    )
    table = h.xpath_element(term_doc, "//table[@width='100%']")
    for row in h.xpath_elements(table, ".//tr[td[@class='td1' or @class='td0']]"):
        crawl_person(context, row, period_start, period_end, is_current_term)


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
    assert len(links) > 1, f"Expected at least 2 links, got {len(links)}"
    # The menu lists the current term twice: first as "Actuels", then as a dated
    # entry (e.g. "56 (2024-2025)"). "Actuels" contains richer member data, so we
    # process it and skip the dated duplicate. URLs are compared without the last character
    # since they differ only by a trailing character.
    if links[0].attrib["href"][:-1] != links[1].attrib["href"][:-1]:
        context.log.warning("Legislature menu structure has changed")
        return

    for link in itertools.chain(links[:1], links[2:]):
        crawl_term(context, link, unblock_validator)
