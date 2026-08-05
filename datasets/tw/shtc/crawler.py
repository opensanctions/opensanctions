import csv
import re

import datapatch
from rigour.mime.types import CSV
from zavod.extract.names.clean import Names
from zavod.extract.zyte_api import fetch_html
from zavod.stateful.review import (
    assert_all_accepted,
)

from zavod import Context, Entity
from zavod import helpers as h

ADDRESS_SPLITS = [
    "Branch Office 1:",
    "Branch Office 2:",
    "Branch Office 3:",
    "Branch Office 4:",
    "Branch Office 5:",
    "Branch Office 6:",
    "Branch Office 7:",
    "Branch Office 8:",
    "Branch Office 9:",
    "Branch Office 10:",
    "Branch Office 11:",
    "Branch Office 12:",
    "Branch Office 13:",
    "Branch Office 14:",
    "Branch Office 15:",
    "Branch Office 16:",
    "viii)",
    "iii)",
    "vii)",
    "iv)",
    "vi)",
    "ii)",
    "i)",
    "v)",
    ";",
]
PERMANENT_ID_RE = re.compile(r"^(?P<name>.+?)（永久參考號：(?P<unsc_num>.+?)）$")


def apply_details_override(
    context: Context, entity: Entity, lookup_result: datapatch.Result
) -> None:
    details = lookup_result.details[0]

    entity.add("name", details.get("name"))
    entity.add_cast("Person", "birthDate", details.get("dob"))
    entity.add_cast("Person", "birthPlace", details.get("pob"))
    entity.add("notes", details.get("notes"))
    entity.add("idNumber", details.get("id_num"))
    entity.add("address", details.get("address"))
    entity.add("email", details.get("email"))
    entity.add("phone", details.get("telephone"))


def parse_names(
    context: Context, item_num: str, entity: Entity, names_raw: str, aliases_raw: str
) -> None:
    # Deal with UNSC numbers in names
    perm_id_match = PERMANENT_ID_RE.match(names_raw.strip())
    if perm_id_match:
        raw_name_without_unsc_id = perm_id_match.group("name").strip()
        unsc_num = perm_id_match.group("unsc_num")
        if unsc_num is not None and len(unsc_num) > 3:
            sanction = h.make_sanction(context, entity)
            sanction.add("unscId", unsc_num)
            context.emit(sanction)
    else:
        raw_name_without_unsc_id = names_raw.strip()

    if "永久參考號" in raw_name_without_unsc_id:
        context.log.warning(
            "Failed to separate name and UNSC number", names_raw=names_raw
        )

    original_names = Names(name=raw_name_without_unsc_id, alias=aliases_raw.strip())
    h.apply_reviewed_names(context, entity, original=original_names)


def crawl_row(context: Context, row: dict[str, str]) -> None:
    # Running number, too unstable to build and ID from.
    item_num = row.pop("項次item", None)
    assert item_num is not None, "Missing item number"
    names_str = row.pop("名稱name").strip()
    aliases_str = row.pop("別名alias").strip()
    if len(names_str) == 0 and len(aliases_str) == 0:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(names_str, aliases_str)

    parse_names(context, item_num, entity, names_str, aliases_str)

    for address in h.multi_split(row.pop("地址address"), ADDRESS_SPLITS):
        if len(address) == 2:
            entity.add("country", address)
            continue
        # Generic override to map more details in the address field
        details_lookup_result = context.lookup("details", address)
        if details_lookup_result is not None:
            apply_details_override(context, entity, details_lookup_result)
        else:
            entity.add("address", address)

    for id_number in row.pop("護照號碼ID Number").split(";"):
        # Generic override to map more details in the ID number field
        details_lookup_result = context.lookup("details", id_number)
        if details_lookup_result is not None:
            apply_details_override(context, entity, details_lookup_result)
        else:
            entity.add("idNumber", id_number)

    for country in row.pop("國家代碼country code").split(";"):
        entity.add("country", country)
    entity.add("topics", "export.control")

    context.emit(entity)
    context.audit_data(row)


def crawl(context: Context) -> None:
    # On the dataset page is a link to a PDF file that contains a link to the CSV file.
    # Assert on the URL of the PDF file in the hope that it changes when the list is updated.
    url_xpath = ".//a[@title='SHTC Entity List']/@href"
    source_url = context.dataset.model.url
    assert source_url is not None, "Dataset model URL is not set"
    doc = fetch_html(
        context,
        source_url,
        url_xpath,
        cache_days=1,
    )
    urls = h.xpath_strings(doc, url_xpath)
    assert len(urls) == 1, 'Expected exactly one document called "SHTC Entity List"'
    # 2025-03-04	SHTC Entity List
    h.assert_url_hash(context, urls[0], "d046359c5be70faccb040a94035bba54faff6e80")

    # Crawl the CSV file
    path = context.fetch_resource("shtc_list.csv", context.data_url)
    context.export_resource(path, mime_type=CSV, title=context.SOURCE_TITLE)
    # utf-8-sig filters out weird Microsoft BOM artifacts
    with open(path, encoding="utf-8-sig") as infh:
        for row in csv.DictReader(infh):
            crawl_row(context, row)

    assert_all_accepted(context, raise_on_unaccepted=False)
