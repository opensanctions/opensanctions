from normality import slugify
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(
    row: dict[str, str | None], context: Context, owner_names: set[str]
) -> None:
    entity = context.make("LegalEntity")
    name = row.pop("excluded_entity")
    entity.id = context.make_id(name, row.get("npi_atypical_id_excluded"))
    entity.add("name", name)
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

    owner_name = row.pop("ownership")
    if slugify(name) in owner_names:
        # Every provider is given an owner, and a practitioner is given as their own,
        # so a provider the list names as an owner is a person. Owning yourself is no
        # relationship, and only a business can be the asset of an Ownership: that
        # expects an Asset, where a plain LegalEntity isn't one.
        entity.add_schema("Person")
    else:
        entity.add_schema("Company")

        owner = context.make("Person")
        owner.id = context.make_id(owner_name)
        owner.add("name", owner_name)

        ownership = context.make("Ownership")
        ownership.id = context.make_id(owner.id, entity.id)

        ownership.add("asset", entity)
        ownership.add("owner", owner)

        context.emit(owner)
        context.emit(ownership)

    context.emit(entity)
    context.emit(sanction)

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
    rows = list(h.parse_xlsx_sheet(context, wb.active, skiprows=6))
    # The list marks a name as a person by putting it in the ownership column.
    owner_names = {
        slug for row in rows if (slug := slugify(row.get("ownership"))) is not None
    }
    for row in rows:
        crawl_item(row, context, owner_names)
