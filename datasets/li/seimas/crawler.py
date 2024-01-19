from typing import Optional
from xml.etree import ElementTree
from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise
from normality import collapse_spaces
from datetime import datetime


def to_date(date_str):
    date_format = "%d %B %Y"
    return datetime.strptime(date_str, date_format)


def clean_text(text, str_to_remove=[]):
    for string in str_to_remove:
        text = text.replace(string, "")

    return text.strip()


def get_occupany_dates(tenure):
    tenure_year = tenure.split()[-1]

    start_year, end_year = tenure_year.split("-")

    return start_year, end_year


def crawl_member_bio(context, url):
    print(f"Processing  {url}")
    doc = context.fetch_html(url, cache_days=1)

    person_name = collapse_spaces(
        doc.xpath('//div[@class="sn_narys_vardas_title"]')[0].text_content()
    )

    date_of_birth = clean_text(
        doc.xpath(
            '//tr[.//*[contains(.//text(), "Date of birth")]]//td//p|//p[.//*[contains(.//text(), "Date of birth")]]'
        )[-1].text_content(),
        str_to_remove=["Date of birth", ","],
    )
    place_of_birth = clean_text(
        doc.xpath(
            '//tr[.//*[contains(.//text(), "Place of birth")]]//td//p|//p[.//*[contains(.//text(), "Place of birth")]]'
        )[-1].text_content(),
        str_to_remove=["Place of birth"],
    )

    position_name = doc.xpath('//div[@class="sn_narys_vardas_title"]')[0].text_content()
    tenure = doc.xpath('//div[@class="kadencija"]')[0].text_content()
    party = (
        doc.xpath('//a[contains(@class, "smn-frakcija")]')[0].text_content()
        if doc.xpath('//a[contains(@class, "smn-frakcija")]')
        else ""
    )

    print(
        {
            "name": person_name,
            "date_of_birth": date_of_birth,
            "place_of_birth": place_of_birth,
            "position": position_name,
            "tenure": tenure,
            "party": party,
        }
    )
    person = context.make("Person")
    person.id = context.make_slug(person_name)
    person.add("name", person_name)
    person.add("sourceUrl", url)
    if date_of_birth:
        person.add("birthDate", to_date(date_of_birth))
    person.add("birthPlace", place_of_birth)

    position_name = position_name.split("from")[0]

    position = h.make_position(
        context,
        f"{position_name}, {party}",
        country="li",
    )

    start_year, end_year = get_occupany_dates(tenure)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_year,
        end_date=end_year,
        # categorisation=categorisation,
    )
    context.emit(person, target=True)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context) -> Optional[str]:
    doc = context.fetch_html(
        "https://www.lrs.lt/sip/portal.show?p_r=35299&p_k=2", cache_days=1
    )

    for anchor in doc.xpath(
        '//div[contains(@class,"list-member")]//a[contains(@class, "smn-name")]'
    ):
        anchor_url = anchor.get("href")
        crawl_member_bio(context, anchor_url)
