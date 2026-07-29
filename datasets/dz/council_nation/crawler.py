import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import LangText

# Mandate lines read e.g. "العهدة التشريعية العاشرة 2025-2031 (19 مايو 2025 => ...)";
# the "YYYY-YYYY" span gives the term's start and end years.
MANDATE_YEARS_RE = re.compile(r"(\d{4})-(\d{4})")

# Safety cap on pagination — the directory is ~11 pages of 15 members.
MAX_PAGES = 40


def collect_member_urls(context: Context) -> list[str]:
    # Every profile link on the listing is a member; paginate until an empty page,
    # keeping insertion order and dropping any repeats across pages.
    urls: dict[str, None] = {}
    for page in range(1, MAX_PAGES + 1):
        doc = context.fetch_html(
            context.data_url,
            params={"page": page},
            cache_days=14,
            absolute_links=True,
        )
        page_urls = h.xpath_strings(doc, '//a[contains(@href, "/members/")]/@href')
        # A page with no member links marks the end of the directory.
        if not page_urls:
            return list(urls)
        urls.update(dict.fromkeys(page_urls))
    raise ValueError("Council members directory exceeded the page cap")


def latest_mandate(text: str) -> tuple[str, str] | None:
    # A member may list several terms; keep the "YYYY-YYYY" span running latest.
    spans: list[tuple[str, str]] = MANDATE_YEARS_RE.findall(text)
    if not spans:
        return None
    return max(spans, key=lambda span: int(span[1]))


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
) -> None:
    member_id = url.rsplit("/", 1)[-1]
    assert member_id, url
    doc = context.fetch_html(url, cache_days=14)

    name = h.element_text(h.xpath_element(doc, '//h1[@class="title"]'))
    assert name, f"Missing name for member {member_id}"

    # The affiliation line is "<party> | <wilaya> (<dates>)".
    affiliation_items = h.xpath_strings(
        doc, '//*[contains(@class, "mbr-party")]//li//text()'
    )
    affiliation = " ".join(" ".join(affiliation_items).split())
    party: str | None = None
    wilaya: str | None = None
    if "|" in affiliation:
        party_part, _, rest = affiliation.partition("|")
        party = party_part.strip() or None
        wilaya = rest.split("(")[0].strip() or None

    mandate_text = " ".join(
        h.xpath_strings(doc, '//*[contains(@class, "mbr-mandate")]//li//text()')
    )
    mandate = latest_mandate(mandate_text)

    person = context.make("Person")
    person.id = context.make_slug(member_id)
    person.add("name", name, lang="ara")
    apply_translit_full_name(context, person, LangText(name, "ara"))
    person.add("political", party, lang="ara")
    person.add("sourceUrl", url)
    # Members must be of Algerian nationality: the elected two-thirds must be sitting
    # local-assembly members (Organic Law 21-01 on the electoral regime, art. 221 -> 220
    # -> art. 184 "être de nationalité algérienne"); 2020 Constitution art. 128 delegates
    # the regime. https://amb-algerie.fr/wp-content/uploads/2021/03/loi-organique-relative-au-regime-electoral.pdf
    person.add("citizenship", "dz")

    start_date = mandate[0] if mandate is not None else None
    end_date = mandate[1] if mandate is not None else None
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    if wilaya is not None:
        occupancy.add("constituency", wilaya, lang="ara")

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Council of the Nation of Algeria",
        country="dz",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21290885",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    for url in collect_member_urls(context):
        crawl_member(context, position, categorisation, url)
