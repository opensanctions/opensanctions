import re
from dataclasses import dataclass
from lxml.html import HtmlElement
from normality import squash_spaces

from zavod import Context, helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise


UNBLOCK_VALIDATOR = "//table[@width='100%']"
POSITION_TOPICS = ["gov.legislative", "gov.national"]

# Matches DOB in plain-text bios:
#   French: "Née à Tournai le 22 mai 1963."
#   Dutch:  "Geboren te Namen op 14 januari 1981."
#   Sometimes the profile on a french page is in Dutch.
BORN_DATE_RE = re.compile(
    r"(Née?\b.+?\ble\b|Geboren\b.+?\bop)\s+(?P<date>\d{1,2}(?:i?er)?\s+\w+\s+\d{4})",
    re.IGNORECASE | re.DOTALL,
)
HEADERS = ["name", "group", "email", "website"]


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


def crawl_person(
    context: Context,
    row: HtmlElement,
    legislature: Legislature,
) -> None:
    name = h.element_text(row["name"])
    profile_url = h.xpath_string(row["name"], ".//a/@href")

    group_texts = h.xpath_strings(row["group"], ".//a/text()")
    political_group = group_texts[0].strip() if group_texts else ""

    context.log.info("Crawling bio", name=squash_spaces(name), profile_url=profile_url)
    pep_doc = zyte_api.fetch_html(
        context,
        profile_url,
        unblock_validator="//table",
        absolute_links=True,
        cache_days=30,
    )
    bio_texts = h.xpath_strings(
        pep_doc,
        './/td/p[contains(., "Né ") or contains(., "Ne ") or contains(., "Née ") or contains(., "Geboren")]/text()',
    )
    if len(bio_texts) > 1:
        context.log.warning(
            f"Multiple potential bio texts found for {name} at {profile_url}"
        )
    bio_text = squash_spaces(bio_texts[0]) if bio_texts else None
    birth_date_match = BORN_DATE_RE.search(bio_text) if bio_text else None
    birth_date = birth_date_match.group("date") if birth_date_match else None

    entity = context.make("Person")
    entity.id = context.make_id(name, birth_date)
    entity.add("name", name)
    entity.add("political", political_group)
    entity.add("sourceUrl", profile_url)
    entity.add("citizenship", "be")
    entity.add("biography", bio_text)
    entity.add("website", h.xpath_strings(row["website"], ".//a/@href"))
    if birth_date:
        h.apply_date(entity, "birthDate", birth_date)

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
        cache_days=1,
    )
    table = h.xpath_element(term_doc, "//table[@width='100%']")
    for tr in h.xpath_elements(table, ".//tr"):
        cells = h.xpath_elements(tr, ".//td")
        row = {hdr: c for hdr, c in zip(HEADERS, cells, strict=True)}
        crawl_person(context, row, legislature)


def crawl(context: Context) -> None:
    # Fetch the main page with all legislature terms
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=UNBLOCK_VALIDATOR,
        absolute_links=True,
        cache_days=1,
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
    # 2. All who have been members this term, including departed and new members.
    #    Note the end year for this listing is incorrect (2025 as of 2026 when they
    #    aren't planning an early election), so we'll discard it.
    #
    # Emitting an occupancy for both listings of the current term means a duplicate
    # occupancy for current members, but it means current members all have a "current" status
    # without incorrectly making known departed members as "current". I think that's
    # better than marking all from this parliamentary term as "current" or "unkown",
    # but we could drop the occupancy for the Actuels page if we wanted to avoid duplicates.
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
