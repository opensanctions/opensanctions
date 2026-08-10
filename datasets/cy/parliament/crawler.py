import re
from urllib.parse import urljoin

from lxml.etree import strip_elements
from zavod.extract.zyte_api import fetch_html
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h

UNBLOCK_VALIDATOR = ".//div[@id='fullpage']"
# The electoral-district index also links to the relevant constitutional text
# ("Μέρος Δεύτερον - Εκλογικαί περιφέρειαι"), which names no members. Matching on
# the district heading picks out only the six pages that list members. The seat
# count in the heading is authoritative; the one in the URL slug goes stale
# (Paphos sits at ".../-πάφου-(4-έδρες)" while returning five members).
DISTRICT = re.compile(r"^Εκλογική περιφέρεια (?P<name>.+?) \((?P<seats>\d+) έδρες\)$")
# Members are numbered contiguously within each district, e.g.
# "1.     Δημητρίου Δημήτρης".
MEMBER = re.compile(r"^(?P<seat>\d+)\.\s+(?P<name>\S.*)$")
# The six districts are fixed by the Constitution, as are the 56 seats they
# return. The remaining 24 of the 80 seats are reserved for the Turkish Cypriot
# community and have been vacant since 1964.
DISTRICT_COUNT = 6


def resolve_name(context: Context, raw: str) -> str | None:
    """Resolve a member name that carries a parenthetical.

    Where a seat changed hands before the House convened, the source names the
    declining candidate in brackets after the sitting member, e.g.
    "Λαούρης Γιάννης (Παναγιώτου Φειδίας)". The bracketed person is a different
    individual who never took the seat — not an alias — so the raw string must
    not reach the entity. Known cases are resolved to the sitting member through
    the `member_names` lookup.
    """
    if "(" not in raw:
        return raw
    name = context.lookup_value("member_names", raw)
    if name is None:
        context.log.warning(
            "Member name contains an unresolved parenthetical. The source uses "
            "brackets to name a candidate who declined the seat, who is a "
            "different person rather than an alias, so this name needs a "
            "`member_names` lookup naming the sitting member.",
            name=raw,
        )
        return None
    return name


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
    district: str,
    party: str,
    raw_name: str,
) -> None:
    name = resolve_name(context, raw_name)
    if name is None:
        return
    person = context.make("Person")
    person.id = context.make_id("cy-mp", name)
    # The source prints names surname-first ("Δημητρίου Αννίτα"). We keep that
    # order rather than guess where a surname ends.
    person.add("name", name, lang="ell")
    person.add("citizenship", "cy")
    person.add("political", party, lang="ell")
    person.add("sourceUrl", url)

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", district, lang="ell")
    occupancy.add("politicalGroup", party, lang="ell")
    context.emit(occupancy)
    context.emit(person)


def crawl_district(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
) -> None:
    doc = fetch_html(
        context,
        url,
        UNBLOCK_VALIDATOR,
        html_source="httpResponseBody",
        cache_days=7,
    )
    article = h.xpath_element(doc, './/section[@id="generic_article"]')
    heading = h.element_text(h.xpath_element(article, ".//h1"))
    match = DISTRICT.match(heading)
    assert match is not None, ("Unexpected district heading", url, heading)
    district = match.group("name")
    seats = int(match.group("seats"))

    content = h.xpath_element(article, './/div[@class="contentdiv"]')
    party: str | None = None
    seen = 0
    for para in list(content.iter("p")):
        # Footnote markers are part of the name paragraph, e.g.
        # "19. Λαούρης Γιάννης (Παναγιώτου Φειδίας)<sup>(1)</sup>". Dropping them
        # also empties the paragraph holding the footnote text itself.
        strip_elements(para, "sup", with_tail=False)
        text = h.element_text(para)
        if not text:
            continue
        # Party headings are a bold run on their own, with the members of that
        # party numbered in the paragraphs below.
        strong = para.find(".//strong")
        if strong is not None and h.element_text(strong) == text:
            party = text
            continue
        member = MEMBER.match(text)
        if member is None:
            continue
        seen += 1
        assert int(member.group("seat")) == seen, (
            "Members are not numbered contiguously",
            url,
            text,
            seen,
        )
        assert party is not None, ("Member listed before any party", url, text)
        crawl_member(
            context,
            position,
            categorisation,
            url,
            district,
            party,
            member.group("name"),
        )
    assert seen == seats, ("Member count does not match seat count", url, seats, seen)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Representatives of Cyprus",
        wikidata_id="Q19801674",
        country="cy",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = fetch_html(
        context,
        context.data_url,
        UNBLOCK_VALIDATOR,
        html_source="httpResponseBody",
        cache_days=1,
    )
    districts = 0
    for link in h.xpath_elements(doc, './/div[@class="greybox"]//a'):
        href = link.get("href")
        if href is None:
            continue
        if DISTRICT.match(h.element_text(link)) is None:
            continue
        districts += 1
        crawl_district(
            context, position, categorisation, urljoin(context.data_url, href)
        )
    assert districts == DISTRICT_COUNT, (
        "Unexpected number of electoral districts",
        districts,
    )
