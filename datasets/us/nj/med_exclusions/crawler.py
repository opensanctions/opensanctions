from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):
    zip_code = row.pop("zip")
    npi = row.pop("npi_number")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(npi, row.get("provider_name"), zip_code)
    entity.add("name", row.pop("provider_name"))
    entity.add("sector", row.pop("title"))
    entity.add("npiCode", h.multi_split(npi.replace("\n", ""), ";/"))
    entity.add("country", "us")

    address = h.make_address(
        context,
        street=row.pop("street"),
        city=row.pop("city"),
        state=row.pop("sta_te"),
        country_code="US",
        postal_code=zip_code,
    )
    h.apply_address(context, entity, address)

    sanction_key = f"{row.get('effective_date')}-{row.get('action')}"
    sanction = h.make_sanction(context, entity, key=sanction_key)
    sanction.add("provisions", row.pop("action"))

    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    h.apply_date(sanction, "endDate", row.pop("expiration_date"))
    end_date = sanction.get("endDate")
    ended = end_date != [] and end_date[0] < context.data_time_iso

    if not ended:
        entity.add("topics", "debarment")

    context.emit(entity, target=not ended)
    context.emit(sanction)
    context.emit(address)

    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", context.data_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for item in h.parse_pdf_table(context, path, headers_per_page=True):
        crawl_item(item, context)
