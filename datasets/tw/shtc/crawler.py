import csv
import re
from typing import Set

import datapatch
from rigour.mime.types import CSV
from zavod.shed.zyte_api import fetch_html

from zavod import Context, Entity
from zavod import helpers as h

ADDRESS_SPLITS = [
    ";",
    "iii)",
    "ii)",
    "i)",
    "iv)",
    "v)",
    "vi)",
    "viii)",
    "vii)",
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
]
NAME_CHINESE = [
    "繁體中文：",  # Traditional Chinese:
    "簡體中文：",  # Simplified Chinese:
]
PERMANENT_ID_RE = re.compile(r"^(?P<name>.+?)（永久參考號：(?P<unsc_num>.+?)）$")


def contains_part(part: str, name: str) -> bool:
    return re.search(r"\b" + re.escape(part) + r"\b", name) is not None


def apply_details_override(
    context: Context, entity: Entity, lookup_result: datapatch.Result
):
    details = lookup_result.details[0]

    entity.add("name", details.get("name"))
    entity.add_cast("Person", "birthDate", details.get("dob"))
    entity.add_cast("Person", "birthPlace", details.get("pob"))
    entity.add("notes", details.get("notes"))
    entity.add("idNumber", details.get("id_num"))
    entity.add("address", details.get("address"))
    entity.add("email", details.get("email"))
    entity.add("phone", details.get("telephone"))


def parse_names(context: Context, entity: Entity, names_str: str, aliases_str: str):
    # Deal with UNSC numbers in names
    perm_id_match = PERMANENT_ID_RE.match(names_str)
    if perm_id_match:
        names_str = perm_id_match.group("name").strip()
        unsc_num = perm_id_match.group("unsc_num")
        if unsc_num is not None and len(unsc_num) > 3:
            sanction = h.make_sanction(context, entity)
            sanction.add("unscId", unsc_num)
            context.emit(sanction)

    if "永久參考號" in names_str:
        context.log.warning(
            "Failed to separate name and UNSC number", names_str=names_str
        )

    aliases_str = aliases_str.replace("<span   id='alias'>", "")
    aliases_str = aliases_str.replace("；", ";")  # Chinese semicolon
    names_str = names_str.replace("；", ";")  # Chinese semicolon

    # In names_str, we sometimes have the aliases appended to the name, e.g. "John Doe alias: John Doe Jr.)"
    # In this case, the aliases are repeated in the aliases_str, so we can just ignore
    # everything after the "alias: " in names_str.
    if " alias:" in names_str:
        names_str, aliases_in_names_str = names_str.split(" alias:", 1)
        aliases_in_names_str = aliases_in_names_str.strip()
        if aliases_in_names_str != aliases_str:
            context.log.warning(
                "Found aliases in names string, but they are not the same as the aliases in the aliases string. "
                "Please check if we need to re-work the crawler to also add aliases from the names string.",
                aliases_in_names_str=aliases_in_names_str,
                aliases_str=aliases_str,
            )

    for split in NAME_CHINESE:
        split = f"; {split}"
        if split in names_str:
            names_str, chinese_name = names_str.split(split, 1)
            entity.add("alias", chinese_name.strip(), lang="zho")

    names_str = names_str.strip()
    entity.add("name", names_str)

    aliases: Set[str] = set()
    for alias in aliases_str.split(";"):
        alias = alias.strip()
        if len(alias) == 0:
            continue
        for split in NAME_CHINESE:
            splitted = alias.split(split, 1)
            if len(splitted) > 1:
                _, chinese_alias = splitted
                entity.add("alias", chinese_alias.strip(), lang="zho")
                break
        else:
            # TODO: Send to review
            if len(alias) < 8:
                entity.add("weakAlias", alias)
                continue
            # TODO: Send to review
            # if " " not in alias:
            #     context.log.warning("Strange alias", alias=alias)
            aliases.add(alias)

    if " " not in names_str and len(aliases):
        prev_name = names_str
        longest_alias = max(aliases, key=len)
        if len(longest_alias) > len(names_str):
            if names_str not in longest_alias:
                aliases.add(names_str)
            names_str = longest_alias
            context.log.info(
                "Promoting longest alias to name", name=names_str, prev_name=prev_name
            )

    entity.add("name", names_str)
    for alias in aliases:
        if alias != names_str:
            entity.add("alias", alias)


def crawl_row(context: Context, row):
    # Running number, too unstable to build and ID from.
    item = row.pop("項次item", None)
    assert item is not None, "Missing item number"
    names_str = row.pop("名稱name").strip()
    aliases_str = row.pop("別名alias").strip()
    if len(names_str) == 0 and len(aliases_str) == 0:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(names_str, aliases_str)

    parse_names(context, entity, names_str, aliases_str)

    for address in h.multi_split(row.pop("地址address"), ADDRESS_SPLITS):
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


def crawl(context: Context):
    # On the dataset page is a link to a PDF file that contains a link to the CSV file.
    # Assert on the URL of the PDF file in the hope that it changes when the list is updated.
    url_xpath = ".//a[@title='SHTC Entity List']/@href"
    doc = fetch_html(
        context,
        context.dataset.model.url,
        url_xpath,
        cache_days=1,
    )
    url = doc.xpath(url_xpath)
    assert len(url) == 1, 'Expected exactly one document called "SHTC Entity List"'
    # 2025-03-04	SHTC Entity List
    h.assert_url_hash(context, url[0], "d046359c5be70faccb040a94035bba54faff6e80")

    # Crawl the CSV file
    path = context.fetch_resource("shtc_list.csv", context.data_url)
    context.export_resource(path, mime_type=CSV, title=context.SOURCE_TITLE)
    # utf-8-sig filters out weird Microsoft BOM artifacts
    with open(path, "rt", encoding="utf-8-sig") as infh:
        for row in csv.DictReader(infh):
            crawl_row(context, row)
