import json
import re
from urllib.parse import urljoin

from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h
from zavod.util import Element

# The site's archive menu has an entry per election, labelled "Senate elections <year>",
# with its results pages under a "Results" submenu. The URL slugs are inconsistent
# across elections, so the menu labels and structure are what we rely on.
SENATE_ELECTION_RE = re.compile(r"^انتخابات مجلس الشيوخ (\d{4})$")
# Each results page loads its per-constituency HTML fragments from this directory.
BASE_URL_RE = re.compile(r"base_url\s*=\s*'([^']+)'")
APPOINTEES_URL = "https://sis.gov.eg/en/media-center/news/president-el-sisi-appoints-100-new-members-to-egypt-s-senate/"


def find_results_page(context: Context) -> tuple[str, str]:
    """Find the final round of the latest Senate election, returning its URL and year."""
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    elections: dict[str, Element] = {}
    for item in h.xpath_elements(doc, ".//ul[contains(@class, 'menutop')]//li[ul]"):
        label = h.element_text(h.xpath_element(item, "./*[1]"))
        match = SENATE_ELECTION_RE.match(label)
        if match is not None:
            elections[match.group(1)] = item
    assert len(elections) > 0, "No Senate elections in the archive menu"
    year = max(elections)
    # The rounds are listed in order, so the last is the final round, whose page lists
    # all the winners.
    pages = h.xpath_strings(
        elections[year],
        "./ul/li[normalize-space(./*[1])='النتائج']/ul/li/a/@href",
    )
    assert len(pages) > 0, f"No results pages for the {year} Senate election"
    return pages[-1], year


def emit_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    *,
    name: str | None,
    lang: str,
    seat_type: str,
    source_url: str,
    period_start: str,
    constituency: str | None = None,
    alias: str | None = None,
    party: str | None = None,
    gender: str | None = None,
) -> None:
    assert name is not None, "Member without a name"
    person = context.make("Person")
    person.id = context.make_id(name, constituency)
    person.add("name", name, lang=lang)
    clean_alias = h.strip_name_titles(context, alias)
    person.add(
        "alias",
        clean_alias,
        lang=lang,
        original_value=alias if clean_alias != alias else None,
    )
    person.add("political", party, lang=lang)
    person.add("gender", gender)
    person.add("sourceUrl", source_url)
    # Senators, elected or appointed, must be Egyptian (Constitution of Egypt, Art. 251).
    # https://www.constituteproject.org/constitution/Egypt_2019
    person.add("citizenship", "eg")

    # The sources are snapshots of the chamber as constituted, not a maintained roster,
    # so no end date doesn't imply the member is still sitting.
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=period_start,
        no_end_implies_current=False,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("summary", seat_type)
    occupancy.add("constituency", constituency, lang="ara")
    context.emit(occupancy)
    context.emit(person)


def fetch_rows(context: Context, url: str, xpath: str) -> list[dict[str, str | None]]:
    """Parse a results table, translating its Arabic headers via the columns lookup."""
    # The fragments are UTF-8 but served without an HTTP charset.
    doc = context.fetch_html(url, cache_days=1, encoding="utf-8")
    table = h.xpath_element(doc, xpath)
    # Matched on the lookup itself, since context.lookup_value swallows the error the
    # required lookup raises for an unknown header.
    columns = context.get_lookup("columns")
    rows: list[dict[str, str | None]] = []
    for cells in h.parse_html_table(table, slugify_headers=False):
        row: dict[str, str | None] = {}
        for heading, text in h.cells_to_str(cells).items():
            key = columns.get_value(heading)
            assert key is not None, heading
            row[key] = text
        rows.append(row)
    return rows


def crawl_individual(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
    constituency: str,
    year: str,
) -> None:
    for row in fetch_rows(context, url, ".//table[@id='winners-table']"):
        emit_member(
            context,
            position,
            categorisation,
            name=row.pop("name"),
            lang="ara",
            seat_type="Individual candidacy",
            source_url=url,
            period_start=year,
            constituency=constituency,
            alias=row.pop("alias"),
            party=row.pop("party"),
        )
        context.audit_data(row)


def crawl_list(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
    constituency: str,
    year: str,
) -> None:
    # Each constituency has a single winning list, whose candidates all take a seat.
    for row in fetch_rows(context, url, ".//table[contains(@class, 'sub-table')]"):
        emit_member(
            context,
            position,
            categorisation,
            name=row.pop("name"),
            lang="ara",
            seat_type="Closed list",
            source_url=url,
            period_start=year,
            constituency=constituency,
            party=row.pop("party"),
            gender=row.pop("gender"),
        )
        # The rank on the list has no bearing on the seat once the list has won.
        context.audit_data(row, ignore=["list_rank"])


def crawl_appointees(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    year: str,
) -> None:
    # The presidential appointees are only published as an English-language news
    # article on the appointment decree, which follows the election within weeks. A
    # new election makes the article stale until it's replaced with the next one.
    doc = context.fetch_html(APPOINTEES_URL, cache_days=1)
    article = json.loads(
        h.xpath_string(
            doc,
            ".//script[@type='application/ld+json'][contains(., 'datePublished')]/text()",
        )
    )
    published = article["datePublished"]
    assert published.startswith(year), (year, published)
    for item in h.xpath_elements(doc, ".//div[@class='item-details-brief']/ol/li"):
        emit_member(
            context,
            position,
            categorisation,
            name=h.element_text(item),
            lang="eng",
            seat_type="Presidential appointee",
            source_url=APPOINTEES_URL,
            period_start=year,
        )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Egypt",
        country="eg",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q98218748",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    results_url, year = find_results_page(context)
    doc = context.fetch_html(results_url, cache_days=1)
    script = h.xpath_string(doc, ".//script[contains(., 'base_url')]/text()")
    base_match = BASE_URL_RE.search(script)
    assert base_match is not None, f"No results fragments directory: {results_url}"
    # The page script resolves the fragment of a constituency code as
    # base_url + 'html/' + code + '.html'.
    fragments_url = urljoin(context.data_url, base_match.group(1))

    # The option values are constituency codes, and the labels their Arabic names.
    for constituency in h.xpath_elements(
        doc, ".//select[@id='individual_consttiuencies']/option[@value!='']"
    ):
        code = h.xpath_string(constituency, "./@value")
        url = urljoin(fragments_url, f"html/{code}.html")
        crawl_individual(
            context, position, categorisation, url, h.element_text(constituency), year
        )
    for constituency in h.xpath_elements(
        doc, ".//select[@id='list_consttiuencies']/option[@value!='']"
    ):
        code = h.xpath_string(constituency, "./@value")
        url = urljoin(fragments_url, f"html/{code}.html")
        crawl_list(
            context, position, categorisation, url, h.element_text(constituency), year
        )

    crawl_appointees(context, position, categorisation, year)
