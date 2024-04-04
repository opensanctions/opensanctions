import re
from lxml import html

from zavod import Context, helpers as h
from zavod.logic.pep import categorise

BASE_URL = "https://www.tbmm.gov.tr"
HEADERS = {
    "Host": "www.tbmm.gov.tr",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Referer": "https://www.tbmm.gov.tr/milletvekili/liste",
}

REGEX_PHONE = re.compile(r"\+\d{2} \(\d{3}\) \d{3} \d{2} \d{2}")


def parse_table(table):
    """This function is used to parse the table of Informations
    about the deputy and return as a dict.
    """

    info_dict = {}

    # The second column is the two dots, so we ignore it
    for row in table.findall(".//tr"):
        label = row.find(".//td[1]").text_content().strip()
        description = row.find(".//td[3]").text_content().strip()
        info_dict[label] = description

    return info_dict


def crawl_item(deputy_url: str, context: Context):

    context.http.get("https://www.tbmm.gov.tr/milletvekili/liste")

    response = context.http.get(deputy_url)

    doc = html.fromstring(response.text)

    name = doc.findtext('.//*[@id="content-title-type"]/span').replace(" - ")

    info_dict = parse_table(doc.find(".//table"))

    entity = context.make("Person")
    entity.id = context.make_id(name)
    entity.add("sourceUrl", deputy_url)

    if "E-Posta" in info_dict:
        entity.add("email", info_dict["E-Posta"])

    if "Telefon" in info_dict:

        phone_numbers = REGEX_PHONE.findall(info_dict["Telefon"])
        for phone_number in phone_numbers:
            entity.add("phone", phone_number)

    if "Adres" in info_dict:
        entity.add("address", info_dict["Adres"])

    position = h.make_position(
        context, "Member of the Grand National Assembly", country="tr"
    )
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        True,
        categorisation=categorisation,
    )

    if occupancy:

        context.emit(entity, target=True)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context):
    # We are going to first get the url for the page of each deputy
    response = context.fetch_html(context.data_url, headers=HEADERS)
    response.make_links_absolute(context.data_url)

    deputies_urls = response.xpath("//a/@href")

    # removing duplicates
    deputies_urls = set(deputies_urls)

    for deputy_url in deputies_urls:
        crawl_item(deputy_url, context)
