import re
from itertools import count

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

MEMBER_HREF_RE = re.compile(r"/members/([0-9a-f]+)$")

# Mandate lines read e.g. "العهدة التشريعية العاشرة 2025-2031 (19 مايو 2025 => ...)";
# the "YYYY-YYYY" span gives the term's start and end years.
MANDATE_YEARS_RE = re.compile(r"(\d{4})-(\d{4})")

# Safety cap on pagination — the directory is ~11 pages of 15 members.
MAX_PAGES = 40


def collect_member_ids(context: Context) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for page in count(1):
        if page > MAX_PAGES:
            raise ValueError("Council members directory exceeded the page cap")
        doc = context.fetch_html(context.data_url, params={"page": page}, cache_days=1)
        page_ids: list[str] = []
        for href in h.xpath_strings(doc, '//a[contains(@href, "/members/")]/@href'):
            match = MEMBER_HREF_RE.search(href)
            if match is not None:
                page_ids.append(match.group(1))
        # A page with no member links marks the end of the directory.
        if not page_ids:
            break
        for member_id in page_ids:
            if member_id not in seen:
                seen.add(member_id)
                ids.append(member_id)
    return ids


def latest_mandate(mandate_texts: list[str]) -> tuple[str, str] | None:
    terms: list[tuple[int, int]] = []
    for text in mandate_texts:
        match = MANDATE_YEARS_RE.search(text)
        if match is not None:
            terms.append((int(match.group(1)), int(match.group(2))))
    if not terms:
        return None
    start, end = max(terms, key=lambda t: t[1])
    return str(start), str(end)


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    member_id: str,
) -> None:
    url = f"https://www.majliselouma.dz/ara/members/{member_id}"
    doc = context.fetch_html(url, cache_days=30)

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

    mandate_texts = [
        " ".join(t.split())
        for t in h.xpath_strings(
            doc, '//*[contains(@class, "mbr-mandate")]//li//text()'
        )
        if t.strip()
    ]
    mandate = latest_mandate(mandate_texts)

    person = context.make("Person")
    person.id = context.make_slug(member_id)
    person.add("name", name, lang="ara")
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
        wikidata_id="Q21290885",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    member_ids = collect_member_ids(context)
    if not member_ids:
        raise ValueError("No members found in the Council directory")
    for member_id in member_ids:
        crawl_member(context, position, categorisation, member_id)
