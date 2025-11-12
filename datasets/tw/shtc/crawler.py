import csv
import re
from typing import List, Set

import datapatch
from pydantic import BaseModel
from rigour.mime.types import CSV
from zavod.shed.zyte_api import fetch_html
from zavod.stateful.review import (
    TextSourceValue,
    assert_all_accepted,
    review_extraction,
)

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
SUSPECT_CHAR_RE = re.compile(r"[()（）]")


class Names(BaseModel):
    primary_name: str
    aliases: List[str] = []
    weak_aliases: List[str] = []


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


def parse_names(
    context: Context, item_num: str, entity: Entity, names_raw: str, aliases_raw: str
):
    primary_name = names_raw.strip()
    # Deal with UNSC numbers in names
    perm_id_match = PERMANENT_ID_RE.match(primary_name)
    if perm_id_match:
        primary_name = perm_id_match.group("name").strip()
        unsc_num = perm_id_match.group("unsc_num")
        if unsc_num is not None and len(unsc_num) > 3:
            sanction = h.make_sanction(context, entity)
            sanction.add("unscId", unsc_num)
            context.emit(sanction)

    if "永久參考號" in primary_name:
        context.log.warning(
            "Failed to separate name and UNSC number", names_raw=names_raw
        )

    aliases_str = aliases_raw.replace("<span   id='alias'>", "")
    aliases_str = aliases_str.replace("；", ";")  # Chinese semicolon
    primary_name = primary_name.replace("；", ";")  # Chinese semicolon
    needs_review = False

    if " alias:" in primary_name:
        primary_name, aliases_in_names_str = primary_name.split(" alias:", 1)
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
        if split in primary_name:
            primary_name, chinese_name = primary_name.split(split, 1)
            entity.add("alias", chinese_name.strip(), lang="zho")

    aliases: Set[str] = set()
    weak_aliases: Set[str] = set()
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
            if len(alias) < 8:
                needs_review = True
                weak_aliases.add(alias)
                continue

            if " " not in alias:
                needs_review = True
            if SUSPECT_CHAR_RE.search(alias):
                needs_review = True
            aliases.add(alias)

    primary_name = primary_name.strip()
    if " " not in primary_name and len(aliases):
        prev_name = primary_name
        longest_alias = max(aliases, key=len)
        if len(longest_alias) > len(primary_name):
            if primary_name not in longest_alias:
                aliases.add(primary_name)
            primary_name = longest_alias
            context.log.info(
                "Promoting longest alias to name",
                name=primary_name,
                prev_name=prev_name,
            )

    names = Names(
        primary_name=primary_name,
        aliases=list(aliases),
        weak_aliases=list(weak_aliases),
    )
    source_text = f"names: {names_raw}\naliases: {aliases_raw}"
    source_value = TextSourceValue(
        key_parts=[names_raw, aliases_raw],
        label="Sanction item",
        text=source_text,
    )
    if needs_review:
        review = review_extraction(
            context,
            source_value=source_value,
            original_extraction=names,
            origin="SHTCEntityList.csv",
        )
        if review.accepted:
            names = review.extracted_data

    entity.add("name", names.primary_name)

    for alias in names.aliases:
        if alias != names.primary_name:
            entity.add("alias", alias)
    for weak_alias in names.weak_aliases:
        entity.add("weakAlias", weak_alias)


def crawl_row(context: Context, row):
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

    assert_all_accepted(context, raise_on_unaccepted=False)
