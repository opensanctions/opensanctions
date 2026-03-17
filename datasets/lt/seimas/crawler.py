from normality import collapse_spaces
from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.util import Element


def get_element_text(
    doc: Element,
    xpath_value: str,
    to_remove: list[str] | None = None,
    position: int = 0,
) -> str:
    elements = h.xpath_elements(doc, xpath_value)
    element_text = h.element_text(elements[position]) if elements else ""

    for string in to_remove or []:
        element_text = element_text.replace(string, "")

    return collapse_spaces(element_text.strip()) or ""


def get_occupany_dates(tenure: str) -> tuple[str, str]:
    tenure_year = tenure.split()[-1]
    start_year, end_year = tenure_year.split("-")

    return start_year, end_year


def crawl_member_bio(context: Context, url: str) -> None:
    doc = context.fetch_html(url, cache_days=1)

    person_name = get_element_text(doc, '//div[@class="sn_narys_vardas_title"]')
    date_of_birth = get_element_text(
        doc,
        '//tr[.//*[contains(.//text(), "Date of birth")]]//td//p|//p[.//*[contains(.//text(), "Date of birth")]]',
        to_remove=["Date of birth", ","],
        position=-1,
    )
    place_of_birth = get_element_text(
        doc,
        '//tr[.//*[contains(.//text(), "Place")]][.//*[contains(.//text(), "birth")]]//td//p|//p[.//*[contains(.//text(), "Place of birth")]]',
        to_remove=["Place of birth"],
        position=-1,
    )

    position_name = get_element_text(doc, '//div[@class="sn-nuo-iki"]')
    position_name = position_name.split("from")[0].strip()

    tenure = get_element_text(doc, '//div[@class="kadencija"]')
    start_year, end_year = get_occupany_dates(tenure)

    person = context.make("Person")
    person.id = context.make_slug(person_name)
    person.add("name", person_name)
    person.add("sourceUrl", url)

    if date_of_birth:
        h.apply_date(person, "birthDate", date_of_birth)
    person.add("birthPlace", place_of_birth)

    position = h.make_position(
        context,
        position_name,
        country="lt",
    )

    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_year,
        end_date=end_year,
        categorisation=categorisation,
    )
    assert occupancy is not None

    context.emit(person)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)

    for anchor_url in h.xpath_strings(
        doc,
        '//div[contains(@class,"list-member")]//a[contains(@class, "smn-name")]/@href',
    ):
        crawl_member_bio(context, anchor_url)
