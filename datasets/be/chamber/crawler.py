from dataclasses import dataclass
from lxml.html import HtmlElement
from normality import squash_spaces

from zavod import Context, helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise


UNBLOCK_VALIDATOR = "//table[@width='100%']"
POSITION_TOPICS = ["gov.legislative", "gov.national"]


@dataclass
class Legislature:
    label: str
    url: str
    start: str | None
    end: str | None
    no_end_implies_current: bool


def get_term_dates(term: str) -> tuple[str, str]:
    _, years = term.split("(", 1)
    years = years.rstrip(")")
    start, end = years.split("-", 1)
    assert len(start) == 4 and len(end) == 4
    int(start)  # just see if this raises an exception
    int(end)
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
    legislature: Legislature,
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
        no_end_implies_current=legislature.no_end_implies_current,
        period_start=legislature.start,
        period_end=legislature.end,
    )
    if occupancy is not None:
        context.emit(entity)
        context.emit(position)
        context.emit(occupancy)


def crawl_term(context: Context, legislature: Legislature) -> None:
    if legislature.start and legislature.start < h.earliest_term_start(POSITION_TOPICS):
        context.log.info(
            f"Skipping term {legislature.label} with start date {legislature.start} outside coverage window"
        )
        return

    # Fetch the member list page for this term
    term_doc = zyte_api.fetch_html(
        context,
        legislature.url,
        unblock_validator=UNBLOCK_VALIDATOR,
        absolute_links=True,
        cache_days=4,
    )
    table = h.xpath_element(term_doc, "//table[@width='100%']")
    for row in h.xpath_elements(table, ".//tr[td[@class='td1' or @class='td0']]"):
        crawl_person(context, row, legislature)


def crawl(context: Context) -> None:
    # Fetch the main page with all legislature terms
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=UNBLOCK_VALIDATOR,
        absolute_links=True,
        cache_days=4,
    )
    # Extract and process legislature terms from menu
    legislature_menu = h.xpath_element(
        doc, './/b[text() = "Actuels"]/ancestor::div[@class="menu"]'
    )

    links = legislature_menu.findall(".//a")

    legislatures = []
    # The menu is a list of parliamentary terms, listing the current term twice:
    # 1. The current membership of the current parliament labeled "Actuels".
    #    I'm hesitant to borrow the start date from the second link without
    #    checking that legis=nn matches between the two.
    # 2. Seems to be the starting members, including departed, and missing new members.
    #    Note the end year for this listing is incorrect (2025 as of 2026 when they
    #    aren't planning an early election), so we'll discard it.
    label = h.element_text(links[0])
    assert label == "Actuels", label
    legislatures.append(
        Legislature(
            label=label,
            url=str(links[0].attrib["href"]),
            start=None,
            end=None,
            no_end_implies_current=True,
        )
    )
    label = h.element_text(links[1])
    legislatures.append(
        Legislature(
            label=label,
            url=str(links[1].attrib["href"]),
            start=get_term_dates(label)[0],
            end=None,  # Discard known incorrect period end date
            no_end_implies_current=False,
        )
    )
    for link in links[2:]:
        label = h.element_text(link)
        start, end = get_term_dates(label)
        legislatures.append(
            Legislature(
                label=label,
                url=str(link.attrib["href"]),
                start=start,
                end=end,
                no_end_implies_current=False,
            )
        )

    for legislature in legislatures:
        crawl_term(context, legislature)
