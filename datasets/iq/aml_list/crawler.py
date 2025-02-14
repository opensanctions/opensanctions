import re
from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h


YEARS_LISTS = [
    "2017",
    "2018",
    "2019",
    "2020",
    "2021",
    "2022",
    "2023",
    "2024",
    "2025",
]
YEAR_PATTERN = re.compile(r"\b(" + "|".join(YEARS_LISTS) + r")\b")


def crawl_row(row: dict, context: Context):
    id = row.pop("t")
    matronymic = row.pop("asm_alam")
    name = row.pop("asm_alshkhs")
    dob = row.pop("altwld")
    decision_number = row.pop("rqm_alqrar")
    listing_date = None
    if decision_number is not None:
        match = YEAR_PATTERN.search(decision_number)
        listing_date = match.group(1) if match else None

    person = context.make("Person")
    person.id = context.make_id(id, name)
    person.add("topics", "debarment")
    h.apply_date(person, "birthDate", dob)
    h.apply_name(
        person,
        full=name,
        matronymic=matronymic,
        lang="ara",
    )
    sanction = h.make_sanction(context, person)
    sanction.add("recordId", decision_number)
    if listing_date:
        sanction.add("listingDate", listing_date)

    context.emit(person)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    url = doc.xpath('//article[@id="post-2171"]//a/@href')
    assert len(url) == 1, url
    url = url[0]
    assert url.endswith(".xlsx"), url
    assert "القوائم-المحلية" in url, url

    path = context.fetch_resource("source.xlsx", url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)
    for year in YEARS_LISTS:
        for row in h.parse_xlsx_sheet(context, wb[year], skiprows=3):
            crawl_row(row, context)
