import csv
from rigour.mime.types import CSV

from zavod import Context, helpers as h
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


def crawl_row(context, row):
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
        result = context.lookup("details", address)
        if result is not None:
            override = result.details[0]
            if override:
                entity.add("address", override.get("address"))
                entity.add("email", override.get("email"))
                entity.add("phone", override.get("telephone"))
        else:
            entity.add("address", address)

    for id in ids.split(";"):
        result = context.lookup("details", id)
        if result is not None:
            override = result.details[0]
            if override:
                entity.add("name", override.get("name"))
                entity.add_cast("Person", "birthDate", override.get("dob"))
                entity.add_cast("Person", "birthPlace", override.get("pob"))
                entity.add("notes", override.get("notes"))
                entity.add("idNumber", override.get("id_num"))
        else:
            entity.add("idNumber", id)

    for name in names.split(";"):
        entity.add("name", name)
    for alias in aliases.split(";"):
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
