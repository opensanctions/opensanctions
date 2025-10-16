import csv
import re
from typing import List, Optional, Tuple
import datapatch
from rigour.mime.types import CSV

from zavod import Context, helpers as h, Entity
from zavod.shed.zyte_api import fetch_html

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
NAME_SPLITS = [
    ";",
    "繁體中文：",  # Traditional Chinese:
    "簡體中文：",  # Simplified Chinese:
    "alias: ",
]
PERMANENT_ID_RE = re.compile(r"^(?P<name>.+?)（永久參考號：(?P<unsc_num>.+?)）$")
# This is not trying to be secure against XSS, it's just basic cleaning of html from a CSV string
HTML_RE = re.compile(r"<[^>]+>")


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


def clean_names(
    context: Context, names_str: str, aliases_str: str
) -> Tuple[List[str], List[str], List[str], Optional[str]]:
    names = []
    aliases = []
    unsc_num = None

    # Deal with UNSC numbers in names
    perm_id_match = PERMANENT_ID_RE.match(names_str)
    if perm_id_match:
        names_str = perm_id_match.group("name")
        unsc_num = perm_id_match.group("unsc_num")
    if "永久參考號" in names_str:
        context.log.warning(
            "Failed to separate name and UNSC number", names_str=names_str
        )

    names_str = HTML_RE.sub("", names_str)
    aliases_str = HTML_RE.sub("", aliases_str)

    split_names = h.multi_split(names_str, NAME_SPLITS)
    # initially just add multipart names
    names.extend([name for name in split_names if len(name.split()) > 1])
    single_part_names = [name for name in split_names if len(name.split()) == 1]

    split_aliases = h.multi_split(aliases_str, NAME_SPLITS)
    # initially just add multipart aliases
    aliases.extend([alias for alias in split_aliases if len(alias.split()) > 1])
    single_part_aliases = [alias for alias in split_aliases if len(alias.split()) == 1]

    for name in single_part_names:
        # Skip single part names that are already in multipart names,
        # e.g. Abdul in Abdul Kader
        if any(contains_part(name, added) for added in names):
            continue
        # Prefer putting single-part names that also occur in aliases into aliases
        if name in single_part_aliases:
            continue
        names.append(name)

    for alias in single_part_aliases:
        # Skip single part alises that are already in a multipart name or alias
        if any(contains_part(alias, added) for added in names):
            continue
        if any(contains_part(alias, added) for added in aliases):
            continue
        aliases.append(alias)

    return names, aliases, unsc_num


def crawl_row(context: Context, row):
    names_str = row.pop("名稱name")
    aliases_str = row.pop("別名alias")
    if not any([names_str, aliases_str]):
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(names_str, aliases_str)

    names, aliases, unsc_num = clean_names(context, names_str, aliases_str)
    entity.add("name", names)
    entity.add("alias", aliases)

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

    if unsc_num:
        sanction = h.make_sanction(context, entity)
        sanction.add("unscId", unsc_num)
        context.emit(sanction)

    context.emit(entity)
    context.audit_data(
        row,
        [
            # Running number, too unstable to build and ID from.
            "項次item"
        ],
    )


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
