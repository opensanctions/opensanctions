from python_calamine import CalamineWorkbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h
from zavod.extract import zyte_api

URL_XPATH = "//*[text()='List of Sanctioned Providers (XLSX)']/ancestor::a"

# The source heads the provenance column of each of its two sanction dates
# "Sanction Source", so the keys come from this list and not from the sheet.
COLUMNS = [
    ("Entity Name", "entity_name"),
    ("Last Name", "last_name"),
    ("First Name", "first_name"),
    ("Middle Name", "middle_name"),
    ("Provider Category", "provider_category"),
    ("NPI#", "npi"),
    ("City", "city"),
    ("License#", "license"),
    ("Sanction Date1", "sanction_date1"),
    ("Sanction Source", "sanction_source1"),
    ("Sanction Date2", "sanction_date2"),
    ("Sanction Source", "sanction_source2"),
    ("Reason", "reason"),
]


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    raw_date_1 = row.pop("sanction_date1")
    sanction_date_1: list[str] | None = raw_date_1.split() if raw_date_1 else None
    raw_date_2 = row.pop("sanction_date2")
    sanction_date_2: list[str] | None = raw_date_2.split() if raw_date_2 else None
    reason = row.pop("reason")
    license = row.pop("license")
    npi = row.pop("npi")
    provider_category = row.pop("provider_category")
    source_1 = row.pop("sanction_source1")
    sources_2 = row.pop("sanction_source2")
    city = row.pop("city")
    address = h.format_address(city=city, state="MI", country_code="US")

    entity = None
    person = None
    if entity_name := row.pop("entity_name"):
        entity = context.make("Company")
        entity.id = context.make_id(entity_name, npi)
        entity.add("name", entity_name)
        entity.add("npiCode", npi)
        entity.add("topics", "debarment")
        entity.add("sector", provider_category)
        entity.add("address", address)
        entity.add("country", "US")
        if license:
            entity.add("description", "License number: " + license)

        sanction = h.make_sanction(context, entity)
        if sanction_date_1:
            h.apply_dates(sanction, "startDate", sanction_date_1)
        if sanction_date_2:
            h.apply_dates(sanction, "startDate", sanction_date_2)
        sanction.add("publisher", source_1)
        sanction.add("publisher", sources_2)
        sanction.add("reason", reason)

        context.emit(entity)
        context.emit(sanction)

    if last_name := row.pop("last_name"):
        person = context.make("Person")
        person.id = context.make_id(
            row.get("first_name"),
            row.get("middle_name"),
            last_name,
            npi,
            city,
        )
        h.apply_name(
            person,
            first_name=row.pop("first_name"),
            middle_name=row.pop("middle_name"),
            last_name=last_name,
        )
        person.add("topics", "debarment")
        person.add("npiCode", npi)
        person.add("sector", provider_category)
        person.add("address", address)
        person.add("country", "US")

        person_sanction = h.make_sanction(context, person)
        if sanction_date_1:
            h.apply_dates(person_sanction, "startDate", sanction_date_1)
        if sanction_date_2:
            h.apply_dates(person_sanction, "startDate", sanction_date_2)
        person_sanction.add("publisher", source_1)
        person_sanction.add("publisher", sources_2)
        person_sanction.add("reason", reason)

        context.emit(person)
        context.emit(person_sanction)

    if last_name and entity_name:
        assert person is not None
        assert entity is not None
        link = context.make("UnknownLink")
        link.id = context.make_id(person.id, entity.id)
        link.add("object", entity)
        link.add("subject", person)

        context.emit(link)

    context.audit_data(row)


def crawl_excel_url(context: Context) -> str:
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        URL_XPATH,
        html_source="httpResponseBody",
        absolute_links=True,
    )
    anchor = h.xpath_element(doc, URL_XPATH)
    href = anchor.get("href")
    assert href, "No link to Excel file found"
    return href


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    _, _, _, path = zyte_api.fetch_resource(
        context, "list.xlsx", url=excel_url, expected_media_type=XLSX
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook = CalamineWorkbook.from_path(path)
    assert len(workbook.sheet_names) == 1, workbook.sheet_names
    sheet = workbook.get_sheet_by_index(0)

    # Row 1 is a title, row 2 holds the headings.
    labels = [str(cell) for cell in sheet.to_python(skip_empty_area=False)[1]]
    assert labels == [label for label, _ in COLUMNS], labels

    headers = [key for _, key in COLUMNS]
    for item in h.parse_xlsx_sheet(context, sheet, skiprows=2, headers=headers):
        crawl_item(item, context)
