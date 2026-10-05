import re
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import LangText

# Khmer digits -> ASCII, to read the years off the page.
KHMER_DIGITS = {0x17E0 + i: str(i) for i in range(10)}
# Navigation label of a legislature, e.g. "<Khmer for legislature no.>7 (2023-2028)".
LEGISLATURE_LABEL = re.compile(r"(\d+)\s*\(\s*(\d{4})\s*-\s*(\d{4})\s*\)\s*$")


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, str | None],
    *,
    period_start: str,
    period_end: str,
    sitting: bool,
) -> None:
    name = row.pop("name")
    party = row.pop("party")
    clean_name = h.strip_name_titles(context, name)
    assert clean_name is not None, name

    person = context.make("Person")
    person.id = context.make_id(name, party)
    person.add(
        "name",
        clean_name,
        original_value=name if clean_name != name else None,
    )
    apply_translit_full_name(context, person, LangText(clean_name, "khm"))
    person.add("political", party)
    person.add("citizenship", "kh")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=period_start,
        period_end=period_end,
        # Only the sitting legislature's list is kept up to date.
        no_end_implies_current=sitting,
    )
    if occupancy is not None:
        occupancy.add("constituency", row.pop("constituency"))
        context.emit(occupancy)
        context.emit(person)

    context.audit_data(row, ignore=["number"])


def crawl_legislature(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
    *,
    period_start: str,
    period_end: str,
    sitting: bool,
) -> None:
    doc = context.fetch_html(url, cache_days=1)
    listing = h.xpath_element(doc, '//table[@id="ContentPlaceHolder1_DListEmp"]')
    # Revisions of the member list, newest first. Take the newest table; some are
    # scanned images, e.g. https://nac.org.kh/article/6531.
    for href in h.xpath_strings(listing, './/a[starts-with(@href, "/article/")]/@href'):
        revision = context.fetch_html(urljoin(url, href.strip()), cache_days=1)
        tables = h.xpath_elements(
            revision,
            '//span[@id="ContentPlaceHolder1_DataList5_FullTextLabel_0"]//table',
        )
        if tables:
            break
    else:
        raise ValueError(f"No list of members is published as a table: {url}")

    for cells in h.parse_html_table(tables[0], header_tag="td", slugify_headers=False):
        row: dict[str, str | None] = {}
        for heading, text in h.cells_to_str(cells).items():
            key = context.lookup_value("columns", heading, warn_unmatched=True)
            if key is not None:
                row[key] = text
        crawl_member(
            context,
            position,
            categorisation,
            row,
            period_start=period_start,
            period_end=period_end,
            sitting=sitting,
        )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Cambodia",
        country="kh",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295974",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    legislatures: dict[int, tuple[str, str, str]] = {}
    for link in h.xpath_elements(doc, '//a[starts-with(@href, "/group-article/")]'):
        match = LEGISLATURE_LABEL.search(h.element_text(link).translate(KHMER_DIGITS))
        if match is not None:
            url = urljoin(context.data_url, h.xpath_string(link, "./@href").strip())
            legislatures[int(match.group(1))] = (match.group(2), match.group(3), url)

    for ordinal, (period_start, period_end, url) in legislatures.items():
        if context.lookup_value("legislatures", str(ordinal)) == "skip":
            continue
        crawl_legislature(
            context,
            position,
            categorisation,
            url,
            period_start=period_start,
            period_end=period_end,
            # The newest legislature is in session.
            sitting=ordinal == max(legislatures),
        )
