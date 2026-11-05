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


def find_elections(context: Context) -> list[tuple[str, list[str]]]:
    """List the Senate elections in the archive menu by year, with their results pages."""
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    elections: list[tuple[str, list[str]]] = []
    for item in h.xpath_elements(doc, ".//ul[contains(@class, 'menutop')]//li[ul]"):
        label = h.element_text(h.xpath_element(item, "./*[1]"))
        match = SENATE_ELECTION_RE.match(label)
        if match is None:
            continue
        pages = h.xpath_strings(
            item, "./ul/li[normalize-space(./*[1])='النتائج']/ul/li/a/@href"
        )
        assert len(pages) > 0, f"No results pages for the {match.group(1)} election"
        elections.append((match.group(1), pages))
    assert len(elections) > 0, "No Senate elections in the archive menu"
    years = [year for year, _ in elections]
    assert len(set(years)) == len(years), years
    return sorted(elections)


def find_results(
    context: Context, pages: list[str], allow_fallback: bool
) -> tuple[Element, str, bool]:
    """Find the latest round with published results, returning its page, the
    directory of its constituency fragments and whether it is the final round."""
    # The rounds are listed in order, and the final round's page lists all the
    # winners. The 2020 runoff page was published without results, so a past
    # election may fall back to an earlier round.
    for page in reversed(pages):
        doc = context.fetch_html(page, cache_days=1)
        scripts = h.xpath_strings(doc, ".//script[contains(., 'base_url')]/text()")
        if len(scripts) == 0 and allow_fallback:
            context.log.info("Results page without results", url=page)
            continue
        assert len(scripts) == 1, f"Expected one results script: {page}"
        base_match = BASE_URL_RE.search(scripts[0])
        assert base_match is not None, f"No results fragments directory: {page}"
        fragments_url = urljoin(context.data_url, base_match.group(1))
        return doc, fragments_url, page == pages[-1]
    raise ValueError(f"No results published: {pages}")


def emit_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    *,
    name: str | None,
    lang: str,
    period_start: str,
    period_end: str | None = None,
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
        period_end=period_end,
        no_end_implies_current=False,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency, lang="ara")
    context.emit(occupancy)
    context.emit(person)


def fetch_rows(
    context: Context, url: str, xpath: str, allow_empty: bool = False
) -> list[dict[str, str | None]]:
    """Parse a results table, translating its Arabic headers via the columns lookup."""
    # The fragments are UTF-8 but served without an HTTP charset.
    doc = context.fetch_html(url, cache_days=1, encoding="utf-8")
    tables = h.xpath_elements(doc, xpath)
    if len(tables) == 0:
        assert allow_empty, f"No results table: {url}"
        # "No winning candidates": all of the constituency's seats went to the runoff.
        no_winners = "لا يوجد مرشحين فائزين" in h.element_text(doc)
        assert no_winners, f"No results table: {url}"
        return []
    assert len(tables) == 1, f"Multiple results tables: {url}"
    table = tables[0]
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
    assert len(rows) > 0, f"Empty results table: {url}"
    return rows


def crawl_individual(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
    constituency: str,
    period: tuple[str, str | None],
    final_round: bool,
) -> None:
    xpath = ".//table[@id='winners-table']"
    for row in fetch_rows(context, url, xpath, allow_empty=not final_round):
        # The 2020 results number the winners instead of giving their alias and party.
        alias: str | None = None
        party: str | None = None
        if row.pop("serial", None) is None:
            alias = row.pop("alias")
            party = row.pop("party")
        emit_member(
            context,
            position,
            categorisation,
            name=row.pop("name"),
            lang="ara",
            period_start=period[0],
            period_end=period[1],
            constituency=constituency,
            alias=alias,
            party=party,
        )
        context.audit_data(row)


def crawl_list(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
    constituency: str,
    period: tuple[str, str | None],
) -> None:
    # Each constituency has a single winning list, whose candidates all take a seat.
    for row in fetch_rows(context, url, ".//table[contains(@class, 'sub-table')]"):
        # The 2020 results give the quota a place on the list was reserved for
        # instead of the gender, which the gender lookup maps for the women's quota.
        gender = row.pop("quota") if "quota" in row else row.pop("gender")
        emit_member(
            context,
            position,
            categorisation,
            name=row.pop("name"),
            lang="ara",
            period_start=period[0],
            period_end=period[1],
            constituency=constituency,
            party=row.pop("party"),
            gender=gender,
        )
        # The rank on the list has no bearing on the seat once the list has won.
        context.audit_data(row, ignore=["list_rank"])


def crawl_election(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    pages: list[str],
    period: tuple[str, str | None],
) -> None:
    # The current term must come from the final round, which lists all its members.
    allow_fallback = period[1] is not None
    doc, fragments_url, final_round = find_results(context, pages, allow_fallback)
    # The option values are constituency codes, and the labels their Arabic names.
    individual = h.xpath_elements(
        doc, ".//select[@id='individual_consttiuencies']/option[@value!='']"
    )
    assert len(individual) > 0, "No individual-candidacy constituencies"
    for constituency in individual:
        code = h.xpath_string(constituency, "./@value")
        url = urljoin(fragments_url, f"html/{code}.html")
        name = h.element_text(constituency)
        crawl_individual(
            context, position, categorisation, url, name, period, final_round
        )
    lists = h.xpath_elements(
        doc, ".//select[@id='list_consttiuencies']/option[@value!='']"
    )
    assert len(lists) > 0, "No closed-list constituencies"
    for constituency in lists:
        code = h.xpath_string(constituency, "./@value")
        url = urljoin(fragments_url, f"html/{code}.html")
        name = h.element_text(constituency)
        crawl_list(context, position, categorisation, url, name, period)


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
    items = h.xpath_elements(
        doc, ".//div[contains(@class, 'item-details-brief')]/ol/li"
    )
    assert len(items) > 0, "No appointees in the article"
    for item in items:
        emit_member(
            context,
            position,
            categorisation,
            name=h.element_text(item),
            lang="eng",
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

    elections = find_elections(context)
    # Each term runs until the chamber elected next takes its place.
    ends: list[str | None] = [year for year, _ in elections[1:]]
    ends.append(None)
    for (year, pages), end in zip(elections, ends):
        crawl_election(context, position, categorisation, pages, (year, end))

    crawl_appointees(context, position, categorisation, elections[-1][0])
