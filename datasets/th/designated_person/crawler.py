from typing import Dict
from lxml.etree import _Element
import re

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html
from normality import collapse_spaces


REGEX_CLEAN_NAME = re.compile(r"^\d\. ?")


def parse_table(
    table: _Element,
) -> Dict[str, str]:
    return {
        row.findtext(".//th"): row.findtext(".//td") for row in table.findall(".//tr")
    }


def clean_name(name: str) -> str:
    return REGEX_CLEAN_NAME.sub("", name)


def crawl_item(url: str, context: Context):
    response = fetch_html(
        context,
        url,
        unblock_validator=".//table",
        html_source="httpResponseBody",
        cache_days=1,
    )

    info_dict = parse_table(response.find(".//table"))
    en_name = info_dict.pop("Individual/Entity Name (English)", "").strip()
    th_name = info_dict.pop("Individual/Entity Name (Thailand)").strip()
    birth_date = info_dict.pop("Date of Birth")
    birth_date_parsed = (h.extract_date(context.dataset, birth_date))[0]
    entity = context.make("Person")
    if not (en_name or clean_name(th_name)):
        return
    entity.id = context.make_slug(en_name or th_name, birth_date_parsed)

    entity.add("name", en_name, lang="en")
    entity.add("name", clean_name(th_name), lang="th")
    entity.add("topics", "sanction")
    entity.add("nationality", info_dict.pop("Nationality", None) or None)
    entity.add("phone", info_dict.pop("Phone Number", None) or None)
    entity.add("address", info_dict.pop("Address No.1", None) or None, lang="th")
    entity.add("address", info_dict.pop("Address No.2", None) or None, lang="th")
    entity.add("email", info_dict.pop("E-mail", None) or None)
    entity.add("sourceUrl", url)
    if birth_date_parsed:
        entity.add("birthDate", birth_date_parsed)
    entity.add(
        "idNumber", info_dict.pop("National Identification Number", None) or None
    )
    entity.add(
        "program",
        "Thailand Counter-Terrorism and Proliferation Financing Act B.E. 2559 section 7, Notificaton Number: {}".format(
            info_dict.pop("Notification Number")
        ),
    )

    context.emit(entity)

    passport_numbers = collapse_spaces(info_dict.pop("Passport Number", None))
    if passport_numbers:
        for passport_number in passport_numbers.split(","):
            passport = h.make_identification(
                context,
                entity,
                passport_number.strip(),
                doc_type="passport",
                country="th",
                passport=True,
            )
            context.emit(passport)

    context.audit_data(info_dict, ignore=["Status"])


def crawl(context: Context):
    response = context.fetch_html(context.data_url, cache_days=1)
    response.make_links_absolute(context.data_url)

    # We are going to iterate over all url of the designated persons
    for a in response.findall(".//table[@id='datatable']/tbody/tr/td/a"):
        crawl_item(a.get("href"), context)
