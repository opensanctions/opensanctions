from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):
    zip_code = row.pop("zip")
    npi = row.pop("npi_number")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(npi, row.get("provider_name"), zip_code)
    entity.add("name", h.multi_split(row.pop("provider_name"), ["a.k.a."]))
    entity.add("sector", row.pop("title"))
    entity.add("npiCode", h.multi_split(npi.replace("\n", ""), ";/"))
    entity.add("country", "us")

    address = h.make_address(
        context,
        street=row.pop("street"),
        city=row.pop("city"),
        state=row.pop("state"),
        country_code="US",
        postal_code=zip_code,
    )
    h.apply_address(context, entity, address)

    sanction_key = f"{row.get('effective_date')}-{row.get('action')}"
    sanction = h.make_sanction(context, entity, key=sanction_key)
    sanction.add("provisions", row.pop("action"))

    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    h.apply_date(sanction, "endDate", row.pop("expiration_date"))

    is_debarred = h.is_active(sanction)
    if is_debarred:
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)
    context.emit(address)

    context.audit_data(row)


def translate_keys(
    context: Context, lookup: str, row: Dict[str, str]
) -> Dict[str, str]:
    return {context.lookup_value(lookup, k, k): v for k, v in row.items()}


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", context.data_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for item in h.parse_pdf_table(context, path, headers_per_page=True):
        if all([v == "" for v in item.values()]):
            continue
        item = translate_keys(context, "headers", item)
        crawl_item(item, context)
