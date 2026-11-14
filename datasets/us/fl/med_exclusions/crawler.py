import re
from collections import Counter
from urllib.parse import urlencode, urljoin

from lxml import html
from python_calamine import CalamineWorkbook
from rigour.mime.types import XLS

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.extract.zyte_api import ZyteAPIRequest

HEADERS = [
    "provider",
    "medicaid_provider_number",
    "license_number",
    "npi_number",
    "provider_type",
    "date_rendered",
    "sanction_type",
    "violation_code",
    "fine_amount",
    "sanction_date",
    "ahca_case_number",
    "formal_informal_case_number",
    "document_type",
]


def crawl_item(context: Context, row: dict[str, str | None]) -> None:
    provider_number = row.pop("medicaid_provider_number")
    name = row.pop("provider")
    assert provider_number is not None and provider_number.isdigit(), row
    assert name is not None, row

    # Names mix people and businesses, and neither the name nor the provider
    # type settles which, so every provider is a LegalEntity.
    entity = context.make("LegalEntity")
    entity.id = context.make_id("fl-medicaid", provider_number)
    entity.add("name", name)
    entity.add("idNumber", provider_number)
    entity.add("npiCode", row.pop("npi_number"))
    entity.add("licenseNumber", row.pop("license_number"))
    entity.add("sector", row.pop("provider_type"))
    entity.add("country", "us")

    sanction_type = row.pop("sanction_type")
    case_number = row.pop("ahca_case_number")
    sanction_date = row.pop("sanction_date")
    assert row.pop("date_rendered") == sanction_date, row
    sanction = h.make_sanction(context, entity, key=f"{case_number}-{sanction_type}")
    h.apply_date(sanction, "startDate", sanction_date)
    sanction.add("provisions", sanction_type)
    sanction.add("authorityId", case_number)
    sanction.add("reason", row.pop("violation_code"))

    if h.is_active(sanction):
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)
    # Fines are out of scope; the document type and the hearing case number
    # describe the order's procedure, not the provider or the sanction.
    context.audit_data(
        row, ignore=["fine_amount", "document_type", "formal_informal_case_number"]
    )


def crawl(context: Context) -> None:
    # The search runs in a cookieless ASP.NET session whose ID only appears in
    # the URL the first request redirects to: /dm_web/(S(<id>))/default.aspx.
    page = zyte_api.fetch(
        context, ZyteAPIRequest(url=context.data_url, geolocation="US")
    )
    assert page.status_code == 200 and page.url is not None, page.url
    assert "/(S(" in page.url, page.url
    doc = html.fromstring(page.response_text)
    form = h.xpath_element(doc, "//form[@id='Form1']")
    data = {
        h.xpath_string(field, "./@name"): field.get("value", "")
        for field in h.xpath_elements(form, ".//input[@type='hidden'][@name]")
    }
    data.update(
        {
            "rdlFOMedState": "Medicaid Sanctioned Providers*",
            "ddlFOType": "ALL",
            "txtPartyName": "",
            "txtBegDate": "",
            "txtEndDate": "",
            "txtAHCACaseNumber": "",
            "txtFICaseNumber": "",
            "btnSearchFO": "Continue",
        }
    )
    results = zyte_api.fetch(
        context,
        ZyteAPIRequest(
            url=page.url,
            method="POST",
            body=urlencode(data).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            geolocation="US",
        ),
    )
    assert results.status_code == 200, results.url
    # The sanctions are an SSRS ReportViewer, which exports from this URL plus
    # a format name. Only EXCEL is enabled.
    bases = re.findall(r'"ExportUrlBase":"([^"]+)"', results.response_text)
    assert len(bases) == 1, bases
    export_url = urljoin(page.url, bases[0].replace("\\u0026", "&") + "EXCEL")
    _, _, _, path = zyte_api.fetch_resource(
        context, "source.xls", export_url, expected_media_type=XLS, geolocation="US"
    )
    context.export_resource(path, XLS, title=context.SOURCE_TITLE)

    sheet = CalamineWorkbook.from_path(path).get_sheet_by_name("FOReport")
    rows = list(h.parse_xlsx_sheet(context, sheet))
    assert len(rows) > 0 and list(rows[0].keys()) == HEADERS, rows[:1]

    # A rescinded order cancels the termination or suspension of the same case.
    rescinded: set[tuple[str | None, str | None, str]] = set()
    for row in rows:
        sanction_type = row["sanction_type"] or ""
        if sanction_type.endswith(" RESCINDED"):
            original = sanction_type.removesuffix(" RESCINDED")
            key = (row["medicaid_provider_number"], row["ahca_case_number"], original)
            rescinded.add(key)

    skipped: Counter[str] = Counter()
    for row in rows:
        raw_type = row["sanction_type"]
        if raw_type is None:
            skipped["(no sanction type)"] += 1
            continue
        scope = context.lookup_value("sanction_type", raw_type, warn_unmatched=True)
        if scope is None:
            continue
        if scope == "skip":
            skipped[raw_type] += 1
            continue
        key = (row["medicaid_provider_number"], row["ahca_case_number"], raw_type)
        if key in rescinded:
            skipped[f"{raw_type} (rescinded)"] += 1
            continue
        crawl_item(context, row)
    # Fines, corrective action plans and rescissions are outside this dataset.
    context.log.info("Skipped orders by sanction type", counts=dict(skipped))
