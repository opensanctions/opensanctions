from normality import slugify
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    excluded_name = row.pop("excluded_entity")
    owner_name = row.pop("ownership")
    assert excluded_name is not None, row
    assert owner_name is not None, row

    # The OWNERSHIP column names the owner(s) of the excluded provider. Where it simply
    # repeats the name in EXCLUDED ENTITY, the excluded provider is that individual, so
    # there is no ownership relationship to model. Otherwise the excluded provider is a
    # business, and the named person owns it.
    is_individual = slugify(excluded_name) == slugify(owner_name)

    # Ownership:asset must reference an Asset, so an owned business is a Company
    # rather than a plain LegalEntity.
    entity = context.make("Person" if is_individual else "Company")
    entity.id = context.make_id(excluded_name, row.get("npi_atypical_id_excluded"))
    entity.add("name", excluded_name)
    entity.add("npiCode", row.pop("npi_atypical_id_excluded"))
    entity.add("topics", "debarment")
    address = h.format_address(
        state=row.pop("state"),
        city=row.pop("city"),
        postal_code=row.pop("zip_code"),
        country_code="US",
    )
    entity.add("address", address)
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    sanction.add("reason", row.pop("reason_for_exclusion"))

    context.emit(entity)
    context.emit(sanction)

    if not is_individual:
        owner = context.make("Person")
        owner.id = context.make_id(owner_name)
        owner.add("name", owner_name)

        ownership = context.make("Ownership")
        ownership.id = context.make_id(owner.id, entity.id)
        ownership.add("asset", entity)
        ownership.add("owner", owner)

        context.emit(owner)
        context.emit(ownership)

    context.audit_data(row)


def crawl_excel_url(context: Context) -> str:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    url = h.xpath_string(doc, "//*[text() = 'State Excluded Provider List']/@href")
    assert url is not None, "Could not find Excel file URL"
    return url


def crawl(context: Context) -> None:
    excel_url = crawl_excel_url(context)

    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    assert wb.active is not None
    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=6):
        crawl_item(item, context)
