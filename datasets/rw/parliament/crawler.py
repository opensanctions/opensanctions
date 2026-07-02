import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element


# The roster renders both chambers on one page, each in its own grid with an
# independent "load more" pagination keyed by a rotating Typo3 cHash, so we follow the
# rendered next link rather than constructing page URLs.
CHAMBERS = {
    "deputies": ("Member of the Chamber of Deputies of Rwanda", "Q21328594"),
    "senate": ("Member of the Senate of Rwanda", "Q21295148"),
}
DOB_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")
TERM_RE = re.compile(r"From\s+(\d{4})\s+to\s+(\d{4})")
MAX_PAGES = 20


def collect_articles(articles: list[Element], members: dict[str, str]) -> None:
    """Record {detail_url: name} for each member article carrying a profile link."""
    for article in articles:
        links = h.xpath_elements(article, ".//a[contains(@href, '/members-details/')]")
        name = article.get("data-member-name")
        href = links[0].get("href") if links else None
        if href is not None and name is not None:
            members[href] = name


def collect_members(context: Context, tab: str) -> dict[str, str]:
    """Follow a chamber grid's load-more chain, returning {detail_url: name}."""
    members: dict[str, str] = {}
    url: str | None = context.data_url
    seen_urls: set[str] = set()
    for _ in range(MAX_PAGES):
        if url is None or url in seen_urls:
            break
        seen_urls.add(url)
        doc = context.fetch_html(url, cache_days=1, absolute_links=True)
        # The chamber's leadership (Speaker/President and their deputies) renders in
        # a "The Bureau" block above the paginated grid, so collect it as well.
        collect_articles(
            h.xpath_elements(
                doc,
                f"//*[@data-admin-panel='{tab}']"
                "//span[normalize-space()='The Bureau']"
                "/following-sibling::div//article[@data-member-name]",
            ),
            members,
        )
        grids = h.xpath_elements(doc, f"//*[@id='{tab}-members-grid']")
        if len(grids) == 0:
            break
        collect_articles(
            h.xpath_elements(grids[0], ".//article[@data-member-name]"), members
        )
        # The chamber's own load-more link carries activeTab=<tab>.
        url = None
        for anchor in h.xpath_elements(doc, "//a[@data-load-more]"):
            href = anchor.get("href")
            if href is not None and f"%5BactiveTab%5D={tab}" in href:
                url = href
                break
    return members


def crawl_member(
    context: Context,
    detail_url: str,
    name: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    doc = context.fetch_html(detail_url, cache_days=7)
    text = h.element_text(doc)

    person = context.make("Person")
    person.id = context.make_slug(detail_url.rstrip("/").split("/")[-1])
    # Names are listed "Hon. SURNAME Given names".
    person.add("name", name.removeprefix("Hon.").strip())
    person.add("sourceUrl", detail_url)
    dob = DOB_RE.search(text)
    if dob is not None:
        h.apply_date(person, "birthDate", dob.group(1))
    # The right to be elected is reserved to Rwandan citizens (Constitution art. 2;
    # detailed eligibility in the organic election law).
    # https://www.constituteproject.org/constitution/Rwanda_2015
    person.add("citizenship", "rw")

    # The chamber field reads e.g. "From 2024 to 2029" — the legislative term.
    term = TERM_RE.search(text)
    period_start = term.group(1) if term is not None else None
    period_end = term.group(2) if term is not None else None
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=period_start,
        period_end=period_end,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("sourceUrl", detail_url)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    for tab, (position_name, wikidata_id) in CHAMBERS.items():
        position = h.make_position(
            context,
            name=position_name,
            country="rw",
            topics=["gov.national", "gov.legislative"],
            wikidata_id=wikidata_id,
            lang="eng",
        )
        categorisation = categorise(context, position, default_is_pep=True)
        context.emit(position)

        members = collect_members(context, tab)
        if len(members) == 0:
            raise ValueError(f"No members found for chamber {tab!r}")
        for detail_url, name in members.items():
            crawl_member(context, detail_url, name, position, categorisation)
