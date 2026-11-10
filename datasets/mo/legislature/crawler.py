import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The source publishes only the sitting legislature and carries no per-member or per-term
# dates — just the legislature number in the listing heading (e.g. "Deputados da 8.ª
# Assembleia Legislativa"). We map each legislature number to the date it took office, so
# occupancies can be dated. When a new legislature is seated the heading number changes;
# the crawler then fails loudly (see crawl()) until the new start date is recorded here,
# rather than silently dating members to the wrong term. The 8th Legislature took office
# on 16 October 2025 for a five-year term.
LEGISLATURE_START = {
    "8": "2025-10-16",
}


def crawl_member(
    context: Context,
    *,
    url: str,
    member_id: str,
    romanised_name: str,
    term_start: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    doc = context.fetch_html(url, cache_days=7, encoding="utf-8")
    detail = h.xpath_element(
        doc, ".//div[contains(concat(' ', @class, ' '), ' member-detail ')]"
    )

    # The detail page is a list of label/value rows. The term-history row appears twice
    # (one copy is a variant for a different layout).
    row: dict[str, str] = {}
    for info_row in h.xpath_elements(
        detail, ".//div[contains(concat(' ', @class, ' '), ' member-info-row ')]"
    ):
        label = h.element_text(
            h.xpath_element(info_row, "./div[@class='member-info-label']")
        )
        row[label] = h.element_text(
            h.xpath_element(info_row, "./div[@class='member-info-value']")
        )

    name_zho = row.pop("姓名")  # Name
    birth = row.pop("出生年份", "")  # Year of birth

    person = context.make("Person")
    person.id = context.make_id(member_id, name_zho)
    person.add("name", name_zho, lang="zho")
    person.add("name", romanised_name)
    h.apply_date(person, "birthDate", birth)
    # Members of the Legislative Assembly are only legally required to be permanent
    # residents of Macau (Basic Law Art. 68); they need not be Chinese nationals — Chinese
    # nationality is required only of the President and Vice-President (Art. 72), and
    # members may hold other nationalities (e.g. Portuguese).
    # https://en.wikisource.org/wiki/Basic_Law_of_the_Macao_Special_Administrative_Region/Chapter_IV/Section_3
    person.add("country", "mo")
    person.add("sourceUrl", url)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=term_start,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(occupancy)
    context.emit(person)
    # The profession, education, other offices, term history, decorations and contact rows
    # are not captured.
    context.audit_data(
        row,
        ignore=[
            "職業",  # Profession
            "學歷",  # Education
            "在其他組織或機構擔任的職務",  # Positions held in other organisations
            "立法會服務年期",  # Terms served in the Legislative Assembly
            "議員辦事處地址",  # Office address
            "電話",  # Telephone
            "電郵",  # Email
            "網頁",  # Website
            "議員社交媒體",  # Social media
            "勳章及獎章",  # Decorations and medals
        ],
    )


def crawl(context: Context) -> None:
    doc = context.fetch_html(
        context.data_url, absolute_links=True, cache_days=1, encoding="utf-8"
    )

    heading = h.element_text(h.xpath_element(doc, ".//div[@class='members-title']"))
    legislature_match = re.search(r"(\d+)\.ª\s*Assembleia\s+Legislativa", heading)
    if legislature_match is None:
        raise ValueError(f"Could not parse legislature from heading: {heading!r}")
    legislature_number = legislature_match.group(1)
    term_start = LEGISLATURE_START.get(legislature_number)
    if term_start is None:
        raise ValueError(
            f"Unknown legislature {legislature_number!r} — a new term has been seated; add "
            "its start date to LEGISLATURE_START."
        )

    position = h.make_position(
        context,
        name="Member of the Legislative Assembly of Macau",
        country="mo",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q28941940",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    seen_member_ids: set[str] = set()
    for member_card in h.xpath_elements(doc, ".//div[@class='members-item']"):
        detail_href = h.xpath_string(member_card, "./a/@href")
        member_id = detail_href.rstrip("/").rsplit("/", 1)[-1].removesuffix(".html")
        if member_id in seen_member_ids:
            continue
        seen_member_ids.add(member_id)
        # The class says "zh", but on the Portuguese listing it holds the romanised name.
        romanised_name = h.element_text(
            h.xpath_element(member_card, ".//div[@class='members-name-zh']")
        )
        crawl_member(
            context,
            url=detail_href.replace("/pt/", "/zh-hant/"),
            member_id=member_id,
            romanised_name=romanised_name,
            term_start=term_start,
            position=position,
            categorisation=categorisation,
        )
