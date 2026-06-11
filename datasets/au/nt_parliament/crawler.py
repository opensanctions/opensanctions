from urllib.parse import urlparse

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.extract import zyte_api
from zavod.util import Element

MEMBERS_URL = "https://parliament.nt.gov.au/members/by-name"
# Member anchors live in the left-hand sub-navigation of the listing page.
MEMBER_LINKS = (
    '//div[contains(@class, "sub-subnav")]/a[contains(@href, "/members/by-name/")]'
)


def clean_member_name(name: str) -> str:
    """Drop the leading "Hon"/"The Hon" courtesy style; keep the rest verbatim.

    "(The) Honourable" is a parliamentary courtesy title applied to ministers and
    members, not part of the personal name; post-nominals (e.g. OAM) are kept.
    """
    for prefix in ("The Hon ", "Hon "):
        if name.startswith(prefix):
            return name[len(prefix) :].strip()
    return name


def detail_field(detail: Element, label: str) -> str | None:
    """Return the value cell of the label/value row whose <th> equals `label`."""
    for row in h.xpath_elements(detail, "//tr[th and td]"):
        th = h.element_text(h.xpath_elements(row, "./th")[0])
        if th == label:
            return h.element_text(h.xpath_elements(row, "./td")[0]) or None
    return None


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    name: str,
    slug: str,
    detail_url: str,
) -> None:
    detail = zyte_api.fetch_html(
        context, detail_url, unblock_validator=".//tr[th]", cache_days=7
    )
    electorate = detail_field(detail, "Electorate")
    party = detail_field(detail, "Party")
    if electorate is None or party is None:
        context.log.warning("Member detail missing electorate/party", url=detail_url)

    person = context.make("Person")
    person.id = context.make_slug("member", slug)
    person.add("name", clean_member_name(name))
    person.add("political", party)
    person.add("sourceUrl", detail_url)
    # Australian citizenship is a legal precondition for an NT MLA, via delegation:
    # Electoral Act 2004 (NT) ss 21 & 32(1)(e) -> Self-Government Act 1978 (Cth) s14
    # -> Commonwealth Electoral Act 1918 (Cth) s93.
    # https://legislation.nt.gov.au/en/Legislation/ELECTORAL-ACT-2004
    person.add("citizenship", "au")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", electorate)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Legislative Assembly of the Northern Territory",
        country="au",
        subnational_area="Northern Territory",
        wikidata_id="Q26998278",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    doc = zyte_api.fetch_html(
        context,
        MEMBERS_URL,
        unblock_validator=MEMBER_LINKS,
        absolute_links=True,
        cache_days=1,
    )
    anchors = h.xpath_elements(doc, MEMBER_LINKS)
    if not (20 <= len(anchors) <= 30):
        # 25 seats; a wild count means the listing layout changed.
        context.log.warning("Unexpected member count", count=len(anchors))
    for anchor in anchors:
        name = h.element_text(anchor)
        detail_url = h.xpath_strings(anchor, "./@href")[0]
        slug = urlparse(detail_url).path.rstrip("/").rsplit("/", 1)[-1]
        crawl_member(context, position, categorisation, name, slug, detail_url)
