import re
from typing import Generator, Dict, Tuple, Optional
from lxml.etree import _Element

from zavod import Context, helpers as h


def parse_table(
    table: _Element,
) -> Dict[str, str]:
    return {
        row.findtext(".//th"): row.findtext(".//td") for row in table.findall(".//tr")
    }


def crawl_item(url: str, context: Context):
    response = context.fetch_html(url)

    info_dict = parse_table(response.find(".//table"))

    en_name = info_dict.pop("Individual/Entity Name (English)").strip()
    th_name = info_dict.pop("Individual/Entity Name (Thailand)").strip()

    entity = context.make("Person")
    entity.id = context.make_id(en_name)

    entity.add("name", en_name, lang="en")
    entity.add("name", th_name, lang="th")
    entity.add("topics", "crime")

    if "Date of Birth" in info_dict:
        entity.add(
            "birthDate",
            h.parse_date(info_dict.pop("Date of Birth"), formats=["%d-%m-%Y"]),
        )

    if "Nationality" in info_dict and info_dict["Nationality"] != "":
        entity.add("nationality", info_dict.pop("Nationality"))

    if "Phone Number" in info_dict and info_dict["Phone Number"] != "":
        entity.add("phone", info_dict.pop("Phone Number"))

    if "E-mail" in info_dict and info_dict["E-mail"] != "":
        entity.add("email", info_dict.pop("E-mail"))

    if "National Identification Number" in info_dict:
        entity.add("idNumber", info_dict.pop("National Identification Number"))

    if "Address No.1" in info_dict:
        entity.add("address", info_dict.pop("Address No.1"), lang="th")

    if "Address No.2" in info_dict:
        entity.add("address", info_dict.pop("Address No.2"), lang="th")

    if "Passport Number" in info_dict and info_dict["Passport Number"] != "":
        entity.add("passportNumber", info_dict.pop("Passport Number"))

    sanction = h.make_sanction(
        context, entity, key=info_dict.pop("Notification Number")
    )

    sanction.add("sourceUrl", url)

    context.audit_data(info_dict, ignore=["Status"])

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    response = context.fetch_html(context.data_url)

    response.make_links_absolute(context.data_url)

    # We are going to iterate over all url of the designated persons
    for a in response.findall(".//table[@id='datatable']/tbody/tr/td/a"):
        crawl_item(a.get("href"), context)
