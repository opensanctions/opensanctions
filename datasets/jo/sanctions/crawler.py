from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from rigour.mime.types import XLSX

IGNORE_FIELDS = [
    "row_no",
    "mother_name",
]


XLSX_XPATH = ".//a[contains(@href, '.xlsx')]"


def crawl_person(context: Context, worksheet: Worksheet) -> None:
    for row in h.parse_xlsx_sheet_openpyxl(
        context,
        worksheet,
        skiprows=2,
        header_lookup=context.get_lookup("columns"),
    ):
        national_id = row.pop("national_id")
        fullname_en = row.pop("full_name_en")
        fullname_ar = row.pop("full_name_ar")
        birth_date = row.pop("birth_date")
        included_date = row.pop("included_date")
        country = row.pop("country")

        person = context.make("Person")
        person.id = context.make_id(
            national_id,
            fullname_ar,
            birth_date,
        )

        person.add("name", fullname_ar, lang="ara")
        person.add("name", fullname_en, lang="eng")
        person.add("country", country)
        person.add("idNumber", national_id)
        person.add("nationality", row.pop("nationality"))
        h.apply_date(person, "birthDate", birth_date)
        person.add("birthPlace", row.pop("birth_place"))
        person.add("notes", row.pop("description"))
        names = row.pop("first_and_surname")
        if names:
            person.add("alias", names.split("،"))
        person.add("sourceUrl", context.data_url)

        address_ent = h.make_address(
            context,
            street=row.pop("area_and_street"),
            city=row.pop("city"),
            country=country,
            lang="ara",
        )
        h.copy_address(person, address_ent)

        doc_type = row.pop("doc_type")
        doc_number = row.pop("doc_no")
        is_passport = (
            context.lookup_value("is_passport", doc_type, warn_unmatched=True) == "true"
        )
        identification = h.make_identification(
            context,
            entity=person,
            number=doc_number,
            doc_type=doc_type,
            start_date=row.pop("doc_issue_date"),
            authority=row.pop("doc_issue_auth"),
            passport=is_passport,
        )

        sanction = h.make_sanction(context, person)
        h.apply_date(sanction, "startDate", included_date)
        if h.is_active(sanction):
            person.add("topics", "sanction")

        context.emit(person)
        context.emit(sanction)
        if identification is not None and doc_number is not None and doc_number != "/":
            context.emit(identification)

        context.audit_data(row, IGNORE_FIELDS)


def crawl_legal_entities(context: Context, worksheet: Worksheet) -> None:
    for row in h.parse_xlsx_sheet_openpyxl(
        context,
        worksheet,
        skiprows=1,
        header_lookup=context.get_lookup("columns"),
    ):
        full_name_en = row.pop("full_name_en")
        included_date = row.pop("included_date")

        legalent = context.make("LegalEntity")
        legalent.id = context.make_id(full_name_en)

        legalent.add("name", row.pop("full_name_ar"), lang="ara")
        legalent.add("name", full_name_en, lang="eng")
        legalent.add("classification", row.pop("classification"), lang="ara")
        legalent.add("sourceUrl", context.data_url)

        sanction = h.make_sanction(context, legalent)
        h.apply_date(sanction, "startDate", included_date)

        context.emit(legalent)
        context.emit(sanction)

        context.audit_data(row, IGNORE_FIELDS)


def crawl(context: Context) -> None:
    # The host only accepts connections from Jordanian IPs
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=XLSX_XPATH,
        html_source="httpResponseBody",
        geolocation="JO",
        absolute_links=True,
    )
    # The same link appears twice, in the desktop and mobile layouts
    urls = set(h.xpath_strings(doc, XLSX_XPATH + "/@href"))
    assert len(urls) == 1, urls
    url = urls.pop()

    _, _, _, path = zyte_api.fetch_resource(
        context, "source.xlsx", url, expected_media_type=XLSX, geolocation="JO"
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)
    assert len(wb.worksheets) == 2

    person_worksheet = wb["الأفراد"]  # "Individuals"
    legal_entity_worksheet = wb["الكيانات"]  # "Entities"

    crawl_person(context, person_worksheet)
    crawl_legal_entities(context, legal_entity_worksheet)
