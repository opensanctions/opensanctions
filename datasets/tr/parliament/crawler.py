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

COOKIES = {
    "TS6d43ef95027": "08ffcef486ab2000c4eb97c00fb5974b83a784e35ed9c80238e4c675dd554055520927fb3bba9d1d080a7c4b5f113000116852f1d1ddd515bd20c2cd5da5d10b9841a4e7f20c8190e864e2d4f3cd4a8b050a7ee5e4dcb4172ae46e363fa26eee",
    "TS8711347c077": "08ffcef486ab2800d4ef7985e124f76edc175ec090f123faab048a0a7c71cfaf27206a9959ae53431c8364f4d04ae53c0841a58d191720007248e9bf00583a51a1566711f8ce2ba9c82a914b23109318b6c0030123067126",
    "TBMM": "!Q+kWfZ7tiLZBh2sMIgyhC/skcd/XaJIw5qljH+4VWyK4YYQkY8iQ2QWTBzto82JJ4OqiSFV+ngxYsQ==",
    "TS01eeca33": "01a57284dbc0776f05981c16e8f8a18e7420af496c4effd01dfbcce4a2ef140499f5fac1d31a501895fa874059f112ead782484262f28888af0244e14f987db65463297d0ea9916f026fd2d0ef411a4ff84922f23f",
    "TSPD_101": "08ffcef486ab2800896890ba44c17ddec4583da7f947a1f135116f3097a2effefad8549adcdfca7e33d3ec2e1e48a616080ff701af051800b03d0db4fc103d32b3671155e4f0f68492f9e33ba79b0147",
    "TSPD_101_DID": "08ffcef486ab2800f970460455742b3efe27f314ef27226b0a3cab60a1f8f542013868bec75c1d7161dac586cbe223d408c3c1388b063800bb2fcb46f7dc65bf1f0a4c95d7277850a31bc801fb0403a754ba71608b47c03bb7a508c5236aa1ca80d351ddb8a5c7d2cac15665b043f533",
    "TS00000000076": "08ffcef486ab2800f970460455742b3efe27f314ef27226b0a3cab60a1f8f542013868bec75c1d7161dac586cbe223d408c3c1388b09d0006e18b8bc78d3119f934be216e781a4cd22405511346fc87dae52b418a7d22acef89caccc665eddc8d0cab135f5b2e59c8877042171f6830ae55178315229e000fcc3ca24ca4ab6c46ef25084b7e1eb7bae8c4b9251e1695abf16000f00221b5b994f4414cce240c91109c66ba898404a6eaec37efa2026cc5f640f5599ecf45ad02b36de5ff2fddd91830e7d2dae14ab43c2e4391eadadeefa0c068283f00d05ec29ebc89a18cb869a973ae8dbd2084fe10044e90581eb646cd1d913c306090a9421c4fe0bab70a4593627728d040f73",
    "X-CSRF-TOKEN-TBMM.WEB.Mvc.Prod": "CfDJ8CkHnVQPiEpDsBJK_N1e34PHppvGDOo-wZx4HU0VNisFZzxsu-kWlCNfCSem82uKbPV04Ldqh7YKetmhNFE-BUK3RD8_fnMQFdkeDNYDaBUMQeXaMtHHu9JY0Ga-Tn76rwiOactMR_PlXO_iz2Y6Azs",
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
    response = context.http.get(deputy_url, headers=HEADERS, cookies=COOKIES)

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
