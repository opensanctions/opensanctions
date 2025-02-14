import re
from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h

SOURCE_URLS = ["https://aml.iq/?page_id=2169", "https://aml.iq/?page_id=2171"]
PERSON_LISTS = [
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
INT_LISTS = ["كيانات", "افراد"]
IGNORE = ["الافراد", "القوائم المحلية "]
YEAR_PATTERN = re.compile(r"\b(" + "|".join(PERSON_LISTS) + r")\b")
COMPANY_NAME_REASON = r"\s*للتوسط ببيع وشراء العملات الاجنبية$"


def crawl_row(row: dict, context: Context):
    entity_name = row.pop("asm_alkyan", None)  # اسم الكيان
    id = row.pop("t")  # ت
    decision_number = row.pop("rqm_alqrar")  # رقم القرار

    reason = None
    if entity_name is not None:
        # Check if the entity name contains the reason
        match = re.search(COMPANY_NAME_REASON, entity_name)
        if match:
            # Extract the reason and strip it from the entity name
            reason = match.group().strip()
            entity_name = re.sub(COMPANY_NAME_REASON, "", entity_name).strip()
    listing_date = None
    if decision_number is not None:
        match = YEAR_PATTERN.search(decision_number)
        listing_date = match.group(1) if match else None

    if entity_name:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(id, entity_name, decision_number)
        entity.add("topics", "debarment")
        entity.add("name", entity_name, lang="ara")
        context.emit(entity)
        sanction = h.make_sanction(context, entity)
    else:
        name = row.pop("asm_alshkhs", row.pop("asma_alashkhas", None))  # اسم الشخص
        person = context.make("Person")
        person.id = context.make_id(id, name)
        person.add("topics", "debarment")
        person.add("nationality", row.pop("aljnsyt", None), lang="ara")  # الجنسية
        h.apply_date(person, "birthDate", row.pop("altwld"))  # التولد
        h.apply_name(
            person,
            full=name,
            matronymic=row.pop("asm_alam"),  # اسم الام
            lang="ara",
        )
        context.emit(person)
        sanction = h.make_sanction(context, person)

    sanction.add("recordId", decision_number)
    sanction.add("reason", reason, lang="ara")
    h.apply_date(sanction, "listingDate", listing_date)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    for url in SOURCE_URLS:
        doc = context.fetch_html(url, cache_days=1)
        # International list
        if "page_id=2169" in url:
            url = doc.xpath('//article[@id="post-2169"]//a/@href')
            assert len(url) == 1, url
            url = url[0]
            assert url.endswith(".xlsx"), url
            assert "القائمة-الدولية" in url, url
            path = context.fetch_resource("international.xlsx", url)
            context.export_resource(path, XLSX, title="international")

            wb = load_workbook(path, read_only=True)
            print(wb.sheetnames)
            for sheet in INT_LISTS:
                for row in h.parse_xlsx_sheet(context, wb[sheet], skiprows=3):
                    crawl_row(row, context)
        # Local lists
        elif "page_id=2171" in url:
            url = doc.xpath('//article[@id="post-2171"]//a/@href')
            assert len(url) == 1, url
            url = url[0]
            assert url.endswith(".xlsx"), url
            assert "القوائم-المحلية" in url, url  # Local lists

            path = context.fetch_resource("local.xlsx", url)
            context.export_resource(path, XLSX, title="local")

            wb = load_workbook(path, read_only=True)
            # Person lists and entity list
            for year in PERSON_LISTS + ["الكيانات"]:  # Entities
                for row in h.parse_xlsx_sheet(context, wb[year], skiprows=3):
                    crawl_row(row, context)
            assert set(wb.sheetnames) == set(PERSON_LISTS + ["الكيانات"] + IGNORE)
