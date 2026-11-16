from hashlib import sha1
from pathlib import Path

from rigour.mime.types import XLSX
from python_calamine import CalamineWorkbook

from zavod import Context
from zavod.extract import zyte_api
from zavod import helpers as h


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    entity = context.make("LegalEntity")
    entity.id = context.make_id(row.get("provider_name"), row.get("npi"))
    entity.add("name", row.pop("provider_name"))
    if row.get("npi") != "N/A":
        entity.add("npiCode", row.pop("npi"))
    else:
        row.pop("npi")
    entity.add("topics", "debarment")
    entity.add("sector", row.pop("prov_type_specialty"))
    if row.get("license") and row.get("license") != "N/A":
        entity.add("description", f"License number: {row.pop('license')}")
    else:
        row.pop("license")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(
        sanction,
        "startDate",
        row.pop("term_date"),
        # Exclusions cannot predate the 1977 Medicare-Medicaid Anti-Fraud and Abuse
        # Amendments.
        two_digit_year_base=1977,
    )
    h.apply_date(sanction, "date", row.pop("letter_date"))
    sanction.add("reason", row.pop("termination_reason"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    # The publisher varies the capitalisation of the upload file name
    # (Sanction-List-... vs sanction-list-...), so match case-insensitively.
    href_lower = (
        "translate(@href, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"
    )
    excel_xpath = (
        f".//a[contains({href_lower}, 'sanction-list')"
        f" and contains({href_lower}, '.xlsx')]/@href"
    )
    landing_page = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=excel_xpath,
        absolute_links=True,
    )
    # The page can link to several copies of the list (WordPress re-uploads get a
    # "-1" suffix). Accept them only if they are byte-identical.
    excel_urls = sorted(set(h.xpath_strings(landing_page, excel_xpath)))
    assert len(excel_urls) > 0, "No sanction list XLSX link found"
    paths: list[Path] = []
    for idx, excel_url in enumerate(excel_urls):
        _, _, _, path = zyte_api.fetch_resource(
            context,
            f"source_{idx}.xlsx",
            excel_url,
            expected_media_type=XLSX,
            geolocation="US",
        )
        paths.append(path)
    checksums = {sha1(p.read_bytes()).hexdigest() for p in paths}
    if len(checksums) != 1:
        raise ValueError(f"Sanction list XLSX links differ in content: {excel_urls}")
    path = paths[0]
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    wb = CalamineWorkbook.from_path(path)
    assert len(wb.sheet_names) == 1, wb.sheet_names
    for item in h.parse_xlsx_sheet(context, wb.get_sheet_by_index(0), skiprows=1):
        crawl_item(item, context)
