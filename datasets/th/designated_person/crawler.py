from typing import Dict
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
    birth_date = info_dict.pop("Date of Birth", None) or None

    entity = context.make("Person")
    entity.id = context.make_slug(en_name, birth_date)

    entity.add("name", en_name, lang="en")
    entity.add("name", th_name, lang="th")
    entity.add("topics", "sanction")

    if birth_date:
        entity.add("birthDate", h.parse_date(birth_date, formats=["%d-%m-%Y"]))

    entity.add("nationality", info_dict.pop("Nationality", None) or None)

    entity.add("phone", info_dict.pop("Phone Number", None) or None)

    entity.add("email", info_dict.pop("E-mail", None) or None)

    entity.add(
        "idNumber", info_dict.pop("National Identification Number", None) or None
    )

    entity.add("address", info_dict.pop("Address No.1", None) or None, lang="th")

    entity.add("address", info_dict.pop("Address No.2", None) or None, lang="th")

    entity.add(
        "program",
        "Thailand Counter-Terrorism and Proliferation Financing Act B.E. 2559 section 7, Notificaton Number: {}".format(
            info_dict.pop("Notification Number")
        ),
    )

    entity.add("sourceUrl", url)

    context.emit(entity, target=True)

    if "Passport Number" in info_dict and info_dict["Passport Number"] != "":
        passport = h.make_identification(
            context,
            entity,
            info_dict.pop("Passport Number"),
            doc_type="passport",
            country="th",
            passport=True,
        )

        context.emit(passport)

    context.audit_data(info_dict, ignore=["Status"])


def crawl(context: Context):
    response = context.fetch_html(context.data_url)

    response.make_links_absolute(context.data_url)

    # We are going to iterate over all url of the designated persons
    for a in response.findall(".//table[@id='datatable']/tbody/tr/td/a"):
        crawl_item(a.get("href"), context)
