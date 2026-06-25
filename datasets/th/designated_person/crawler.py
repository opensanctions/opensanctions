import re

from zavod.extract.zyte_api import fetch_html
from zavod.util import Element

from zavod import Context
from zavod import helpers as h

REGEX_CLEAN_NAME = re.compile(r"^\d\. ?")
PASSPORT_SPLITS = [",", "1.", " 2.", " 3. ", " 4."]


def parse_table(
    table: Element,
) -> dict[str, str | None]:
    # The detail page is a vertical key-value table: each row pairs a <th> label
    # with its <td> value, so we map labels to cell text rather than using the
    # header-row-oriented h.parse_html_table.
    result: dict[str, str | None] = {}
    for row in h.xpath_elements(table, ".//tr"):
        key_el = row.find(".//th")
        if key_el is None:
            continue
        result[h.element_text(key_el)] = h.element_text(row.find(".//td")) or None
    return result


def clean_name(name: str) -> str:
    return REGEX_CLEAN_NAME.sub("", name)


def crawl_item(url: str, context: Context) -> None:
    response = fetch_html(
        context,
        url,
        unblock_validator=".//table",
        html_source="httpResponseBody",
        cache_days=1,
    )

    table = response.find(".//table")
    assert table is not None, "No table found in response"
    info_dict = parse_table(table)
    en_name = info_dict.pop("Individual/Entity Name (English)", None) or ""
    th_name = info_dict.pop("Individual/Entity Name (Thailand)") or ""
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

    passport_numbers = info_dict.pop("Passport Number", None) or ""
    if passport_numbers:
        for passport_number in h.multi_split(passport_numbers, PASSPORT_SPLITS):
            passport = h.make_identification(
                context,
                entity,
                passport_number.strip(),
                doc_type="passport",
                country="th",
                passport=True,
            )
            if passport is not None:
                context.emit(passport)

    context.audit_data(info_dict, ignore=["Status"])


def crawl(context: Context) -> None:
    response = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)

    # We are going to iterate over all url of the designated persons
    for url in h.xpath_strings(
        response, ".//table[@id='datatable']/tbody/tr/td/a/@href"
    ):
        crawl_item(url, context)
