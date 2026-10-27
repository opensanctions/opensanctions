from rigour.mime.types import XLSX
from python_calamine import CalamineWorkbook

from zavod import Context, helpers as h
from zavod.extract import zyte_api


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    name = row.pop("provider_name")
    npi = row.pop("npi")
    if not name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, npi)
    entity.add("name", name)
    entity.add("country", "us")
    entity.add("npiCode", h.multi_split(npi, ["; ", "&", " and "]))
    entity.add("alias", row.pop("doing_business_as_name"))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("termination_effective_date"))
    sanction.add("reason", row.pop("termination_authority"))
    h.apply_date(sanction, "endDate", row.pop("reinstatement_effective_date", None))
    is_active = h.is_active(sanction)
    if is_active:
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context) -> str:
    link_xpath = ".//a[contains(., 'Terminated Provider List')][@href]"
    doc = zyte_api.fetch_html(
        context, context.data_url, link_xpath, absolute_links=True
    )
    href = h.xpath_element(doc, link_xpath).get("href")
    assert href is not None
    return href


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    _, _, _, path = zyte_api.fetch_resource(
        context, filename="source.xlsx", url=excel_url, expected_media_type=XLSX
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook = CalamineWorkbook.from_path(path)

    # Currently terminated providers
    terminated = workbook.get_sheet_by_name("Termination List")
    for item in h.parse_xlsx_sheet(context, terminated):
        crawl_item(item, context)

    # Providers that where terminated but are now reinstated
    if "Reinstated Providers" in workbook.sheet_names:
        reinstated = workbook.get_sheet_by_name("Reinstated Providers")
        for item in h.parse_xlsx_sheet(context, reinstated):
            crawl_item(item, context)
    sheet_names = set(workbook.sheet_names)
    assert len(sheet_names - {"Termination List", "Reinstated Providers"}) == 0
