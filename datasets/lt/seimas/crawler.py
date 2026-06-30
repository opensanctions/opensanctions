from datetime import datetime
import re
from normality import collapse_spaces
from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise, get_after_office
from zavod.util import Element

POSITION_TOPICS = ["gov.legislative", "gov.national"]
CUTOFF_DATE = (datetime.now() - get_after_office(POSITION_TOPICS)).year


URL_PREV_SEIMAS = "https://www.lrs.lt/sip/portal.show?p_r=35357&p_k=2"


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
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator='//div[@class="sn_narys_vardas_title"]',
        html_source="httpResponseBody",
        cache_days=1,
        absolute_links=True,
    )

    # some pages do not list party names, hence None check
    party_list = h.xpath_strings(
        doc, '//div[@class="frakcija"]/a[contains(@class, "smn-frakcija link")]/text()'
    )
    party = party_list[0] if party_list else None

    bio_table = h.xpath_strings(
        doc, '//div[@id="sn_vidines_biografija"]//table//text()'
    )
    bio = collapse_spaces(" ".join(bio_table))

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
    # Parliamentary term dates are not necessarily the same as candidate's occupancy dates
    period_start, period_end = get_occupany_dates(tenure)

    person = context.make("Person")
    person.id = context.make_slug(person_name)
    person.add("citizenship", "lt")
    person.add("name", person_name)
    person.add("biography", bio)
    person.add("political", party)
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
        period_start=period_start,
        period_end=period_end,
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)


def xpath_match(doc: Element, xpaths: list[str]) -> str | None:
    for xpath in xpaths:
        try:
            return h.xpath_string(doc, xpath)
        except ValueError:
            continue
    return None


def crawl_old_member_bio(context: Context, url: str) -> None:
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator='//table[contains(@summary, "Kadencijos")]',
        html_source="httpResponseBody",
        cache_days=1,
    )

    person_name = h.xpath_string(
        doc, '//table[contains(@summary, "Kadencijos")]//font[@size="4"]/text()'
    )  # TODO: more robust name fetch method
    assert person_name is not None

    # Birth details are missing or differently structured on many older member
    # pages, so extract them leniently and emit the member even when absent.
    date_of_birth = xpath_match(
        doc,
        [
            '//div[@class="par"][contains(text(),"Biography")]/following-sibling::div[@align="justify"][1]//table//tr[1]/td[last()]/p[1]/text()'
        ],
    )
    place_of_birth = xpath_match(
        doc,
        [
            '//div[@class="par"][contains(text(),"Biography")]/following-sibling::div[@align="justify"][1]//table//tr[1]/td[last()]/p[1]//strong',
            '//div[@class="par"][contains(text(),"Biography")]/following-sibling::div[@align="justify"][1]//table//tr[1]/td[last()]/p[1]',
        ],
    )

    person = context.make("Person")
    person.id = context.make_slug(person_name)
    person.add("name", person_name)
    person.add("birthPlace", place_of_birth)
    if date_of_birth is not None:
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

    categorisation = categorise(context, position)
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


def crawl(context: Context) -> None:
    ### === crawl current legislature === ###
    members_list_validator = (
        '//div[contains(@class,"list-member")]//a[contains(@class, "smn-name")]'
    )
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=members_list_validator,
        html_source="httpResponseBody",
        cache_days=1,
        absolute_links=True,
    )

    for anchor_url in h.xpath_strings(doc, members_list_validator + "/@href"):
        crawl_member_bio(context, anchor_url)

    ### === crawl older legislatures === ###
    # navigate the landing page that contains a table with older seimas
    doc_landing_older_seimas = zyte_api.fetch_html(
        context,
        URL_PREV_SEIMAS,
        unblock_validator='//div[contains(@class, "rubrika-kvadratai-item")]',
        html_source="httpResponseBody",
        cache_days=1,
        absolute_links=True,
    )
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

        # the overview page layout differs between modern (>=2016) and older legislatures,
        # so the link to the members list — and thus the unblock validator — differs too.
        if int(start_year) >= 2016:
            members_link_xpath = '//div[contains(@class,"rubrika-kvadratai-item")]//a[@title="Members of the Seimas"]'
        else:
            members_link_xpath = '//*[@id="td_kaire"]//a[@class="medis" and text()="Members of the Seimas"]'

        doc_seimas_overview = zyte_api.fetch_html(
            context,
            seimas_url,
            unblock_validator=members_link_xpath,
            html_source="httpResponseBody",
            cache_days=1,
            absolute_links=True,
        )

        members_url = h.xpath_string(doc_seimas_overview, members_link_xpath + "/@href")

        # the seimas webpage is similar to the current seimas for years starting with 2016, recycle the function:
        if int(start_year) >= 2016:
            # visit the older legislature page listing its members
            doc_seimas = zyte_api.fetch_html(
                context,
                members_url,
                unblock_validator=members_list_validator,
                html_source="httpResponseBody",
                cache_days=1,
                absolute_links=True,
            )

            for anchor_url in h.xpath_strings(
                doc_seimas, members_list_validator + "/@href"
            ):
                crawl_member_bio(context, anchor_url)

        # older seimas webpages require a bit more love
        else:
            old_members_validator = (
                '//div[contains(@id, "divDesContent")]//table//a[@href]'
            )
            doc_seimas = zyte_api.fetch_html(
                context,
                members_url,
                unblock_validator=old_members_validator,
                html_source="httpResponseBody",
                cache_days=1,
                absolute_links=True,
            )

            for anchor in h.xpath_elements(doc_seimas, old_members_validator):
                old_member_url = anchor.get("href")
                assert old_member_url is not None

                crawl_old_member_bio(context, old_member_url)
