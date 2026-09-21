import string
from urllib.parse import urlencode, urljoin

from openpyxl import load_workbook
from rigour.mime.types import XLSX
from zavod.extract import zyte_api
from zavod.stateful.review import assert_all_accepted

from zavod import Context
from zavod import helpers as h

EXPORT_PATH = "Search/ExportResults"


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    zip_code = row.pop("zip")
    npi = row.pop("npi_number")
    name_raw = row.pop("provider_name")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(npi, name_raw, zip_code)
    h.apply_reviewed_name_string(context, entity, string=name_raw, llm_cleaning=True)
    entity.add("sector", row.pop("title"))
    entity.add("npiCode", h.multi_split((npi or "").replace("\n", ""), ";/"))
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
    if address is not None:
        context.emit(address)

    context.audit_data(row)


def crawl(context: Context) -> None:
    export_url = urljoin(context.data_url, EXPORT_PATH)
    for letter in string.ascii_lowercase:
        url = f"{export_url}?{urlencode({'Names[0]': letter})}"
        _, _, _, path = zyte_api.fetch_resource(
            context,
            f"source-{letter}.xlsx",
            url,
            expected_media_type=XLSX,
            geolocation="US",
        )

        workbook = load_workbook(path, read_only=True)
        assert workbook.active is not None
        for item in h.parse_xlsx_sheet(context, workbook.active, skiprows=7):
            crawl_item(item, context)

    assert_all_accepted(context, raise_on_unaccepted=False)
