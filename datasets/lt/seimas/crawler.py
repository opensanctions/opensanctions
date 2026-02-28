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
    person.add("citizenship", "lt")

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


def xpath_match(doc, xpaths):
    for xpath in xpaths:
        try:
            return h.xpath_string(doc, xpath)
        except ValueError:
            continue
        return None


def crawl_old_member_bio(context: Context, url: str) -> None:
    doc = context.fetch_html(url, cache_days=1)

    person_name = h.xpath_string(
        doc, '//table[contains(@summary, "Kadencijos")]//font[@size="4"]/text()'
    )  # TODO: more robust name fetch method
    # person_name = person_name.encode("latin1").decode("utf-8")  # correct encoding
    assert person_name is not None

    date_of_birth = None
    place_of_birth = None

    try:
        date_of_birth = h.xpath_string(
            doc,
            '//div[@class="par"][contains(text(),"Biography")]/following-sibling::div[@align="justify"][1]//table//tr[1]/td[last()]/p[1]/text()',
        )

        # OLD place_of_birth_xpaths = ['//div[@class="par"][contains(text(),"Biography")]/following-sibling::div[@align="justify"][1]//table//tr[2]/td[last()]/p[1]/text()',
        #                             '//div[@class="par"][contains(text(),"Biography")]/following-sibling::div[@align="justify"][1]//table//tr[1]/td[last()]/p[2]/text()']

        place_of_birth_xpaths = [
            '//div[@class="par"][contains(text(),"Biography")]/following-sibling::div[@align="justify"][1]//table//tr[1]/td[last()]/p[1]//strong',
            '//div[@class="par"][contains(text(),"Biography")]/following-sibling::div[@align="justify"][1]//table//tr[1]/td[last()]/p[1]',
        ]
        place_of_birth = xpath_match(doc, place_of_birth_xpaths)

    except ValueError:
        print(person_name, date_of_birth, place_of_birth)
        return None

    person = context.make("Person")
    # print(person_name)
    person.id = context.make_slug(person_name)
    person.add("name", person_name)
    person.add("birthPlace", place_of_birth)
    h.apply_date(person, "birthDate", date_of_birth)

    party_affiliation = h.xpath_strings(
        doc,
        '//b[contains(text(), "Political Groups of the Seimas")]/following-sibling::ul[1]//li/a/text()',
    )
    person.add("political", party_affiliation)

    person.add("sourceUrl", url)
    person.add("citizenship", "lt")

    seimas_position_dates = h.xpath_strings(
        doc, '//table[contains(@summary, "Kadencijos")]//b/text()'
    )
    position = h.make_position(
        context,
        name="Member of the Seimas",
        wikidata_id="Q18507240",
        country="lt",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )

    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person=person,
        position=position,
        start_date=seimas_position_dates[1],
        end_date=seimas_position_dates[2],
        categorisation=categorisation,
    )

    if occupancy is not None:
        context.emit(occupancy)
        context.emit(position)
        context.emit(person)


def crawl(context: Context):
    ### === crawl current legislature === ###
    doc = context.fetch_html(context.data_url, cache_days=1)

    for anchor in doc.xpath(
        '//div[contains(@class,"list-member")]//a[contains(@class, "smn-name")]'
    ):
        anchor_url = anchor.get("href")
        crawl_member_bio(context, anchor_url)

    ### === crawl older legislatures === ###
    # navigate the landing page that contains a table with older seimas
    doc_landing_older_seimas = context.fetch_html(URL_PREV_SEIMAS, cache_days=1)
    older_seimas_table = h.xpath_elements(
        doc_landing_older_seimas, '//div[contains(@class, "rubrika-kvadratai-item")]'
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

        # visit the url of an older legislature landing page
        seimas_url = seimas_el.get("href")
        assert seimas_url is not None, "Coundn't fetch URL for the legislature"
        doc_seimas_overview = context.fetch_html(
            seimas_url, cache_days=1, absolute_links=True
        )

        # the seimas webpage is similar to the current seimas for years starting with 2016, recycle the function:
        if int(start_year) >= 2016:
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

        # older seimas webpages require a bit more love
        else:
            # get the URL to the list of Members of the Seimas
            members_url = h.xpath_string(
                doc_seimas_overview,
                '//*[@id="td_kaire"]//a[@class="medis" and text()="Members of the Seimas"]/@href',
            )
            doc_seimas = context.fetch_html(
                members_url, cache_days=1, absolute_links=True
            )

            for anchor in h.xpath_elements(
                doc_seimas, '//div[contains(@id, "divDesContent")]//table//a[@href]'
            ):
                anchor_url = anchor.get("href")
                assert anchor_url is not None

                crawl_old_member_bio(context, anchor_url)
