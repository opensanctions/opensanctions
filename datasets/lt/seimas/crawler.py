from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise
from normality import collapse_spaces
from datetime import datetime
from xml.etree import ElementTree


def to_date(date_str: str) -> datetime:
    date_format = "%d %B %Y"
    return datetime.strptime(date_str, date_format)


def get_element_text(doc: ElementTree, xpath_value: str, to_remove=[], position=0):
    element_tag = doc.xpath(xpath_value)
    element_text = element_tag[position].text_content() if element_tag else ""

    for string in to_remove:
        element_text = element_text.replace(string, "")

    return collapse_spaces(element_text.strip())


def get_occupany_dates(tenure: str):
    tenure_year = tenure.split()[-1]
    start_year, end_year = tenure_year.split("-")

    return start_year, end_year


def crawl_member_bio(context: Context, url: str):
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
        person.add("birthDate", to_date(date_of_birth))
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

    context.emit(person, target=True)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)

    for anchor in doc.xpath(
        '//div[contains(@class,"list-member")]//a[contains(@class, "smn-name")]'
    ):
        anchor_url = anchor.get("href")
        crawl_member_bio(context, anchor_url)
