from datetime import datetime
import re
from normality import collapse_spaces
from xml.etree import ElementTree

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise, get_after_office

POSITION_TOPICS = ["gov.legislative", "gov.national"]
CUTOFF_DATE = (datetime.now() - get_after_office(POSITION_TOPICS)).year


URL_PREV_SEIMAS = "https://www.lrs.lt/sip/portal.show?p_r=35357&p_k=2"


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

    context.emit(person)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context):
    ### === crawl current legislature === ###
    doc = context.fetch_html(context.data_url, cache_days=1)

    for anchor in doc.xpath(
        '//div[contains(@class,"list-member")]//a[contains(@class, "smn-name")]'
    ):
        anchor_url = anchor.get("href")
        crawl_member_bio(context, anchor_url)

    ### === crawl older legislatures === ###
    doc_all_older_seimas = context.fetch_html(URL_PREV_SEIMAS, cache_days=1)
    older_seimas_table = h.xpath_elements(
        doc_all_older_seimas, '//div[contains(@class, "rubrika-kvadratai-item")]'
    )
    assert older_seimas_table is not None

    for seimas in older_seimas_table:
        seimas_el = h.xpath_element(seimas, ".//a")
        seimas_dates_match = re.search(r"\(\d{4}[–-]\d{4}\)", h.element_text(seimas_el))

        assert seimas_dates_match is not None
        seimas_dates = seimas_dates_match.group(0).strip("()")
        start_year, end_year = seimas_dates.split("–")

        # don't collect seimas data beyond the CUTOFF_DATE
        if int(end_year) < CUTOFF_DATE:
            continue

        seimas_url = seimas_el.get("href")
        assert seimas_url is not None, "Coundn't fetch URL for the legislature"

        # visit the url of an older legislature landing page
        doc_seimas_overview = context.fetch_html(seimas_url, cache_days=1)
        # get the URL to the list of Members of the Seimas
        members_url = h.xpath_string(
            doc_seimas_overview,
            '//div[contains(@class,"rubrika-kvadratai-item")]//a[@title="Members of the Seimas"]/@href',
        )
        # visit the older legislature page listing its members
        doc_seimas = context.fetch_html(members_url, cache_days=1)

        for anchor in doc_seimas.xpath(
            '//div[contains(@class,"list-member")]//a[contains(@class, "smn-name")]'
        ):
            anchor_url = anchor.get("href")
            crawl_member_bio(context, anchor_url)
