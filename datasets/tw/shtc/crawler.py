import csv
import re
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
]
PERMANENT_ID_RE = re.compile(r"^(?P<name>.+?)（永久參考號：(?P<reg_number>.+?)）")


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


def crawl_row(context: Context, row):
    row = {
        # BOM in the middle of a file, probably a Microsoft artifact, should be ignored,
        # see https://www.unicode.org/faq/utf_bom.html#bom6
        k.lstrip("\ufeff"): v.strip() if isinstance(v, str) else v
        for k, v in row.items()
    }
    names = row.pop("名稱name")
    aliases = row.pop("別名alias")
    addresses = row.pop("地址address")
    ids = row.pop("護照號碼ID Number")
    if not any([names, aliases]):
        return
    entity = context.make("LegalEntity")
    entity.id = context.make_id(names, aliases)

    for address in h.multi_split(addresses, ADDRESS_SPLITS):
        # Generic override to map more details in the address field
        details_lookup_result = context.lookup("details", address)
        if details_lookup_result is not None:
            apply_details_override(context, entity, details_lookup_result)
        else:
            entity.add("address", address)

    for id_number in ids.split(";"):
        # Generic override to map more details in the ID number field
        details_lookup_result = context.lookup("details", id_number)
        if details_lookup_result is not None:
            apply_details_override(context, entity, details_lookup_result)
        else:
            entity.add("idNumber", id_number)

    match = PERMANENT_ID_RE.match(names)
    if match:
        entity.add("name", match.group("name").strip())
        entity.add("registrationNumber", match.group("reg_number").strip())
    else:
        for name in h.multi_split(names, NAME_SPLITS):
            entity.add("name", name)
    for alias in h.multi_split(aliases, NAME_SPLITS):
        if len(alias.split()) == 1 and len(alias) < 7:
            entity.add("weakAlias", alias)
        else:
            entity.add("alias", alias)
    for country in row.pop("國家代碼country code").split(";"):
        entity.add("country", country)
    entity.add("topics", "export.control")

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
    with open(path, "rt", encoding="utf-8") as infh:
        for row in csv.DictReader(infh):
            crawl_row(context, row)
