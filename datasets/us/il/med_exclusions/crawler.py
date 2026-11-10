import base64
import json
import re
from typing import Any

from normality import collapse_spaces
from python_calamine import CalamineWorkbook
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h

BASE_URL = "https://ilhfspartner3.dynamics365portals.us"
# Only a legal form makes a name certainly a company. Anything less certain is
# emitted as LegalEntity, so it is matched by both person and company queries.
REGEX_LEGAL_FORM = re.compile(
    r"\b(INC|INCORPORATED|L\.?L\.?C|P\.?L\.?L\.?C|CORP|CORPORATION|CO|COMPANY|LTD|"
    r"LIMITED|S\.?C|P\.?C|P\.?A)\b|LLC$"
)
# Business words without a legal form are often sole proprietors trading under
# their own name ("DAVIS DAN TRANSPORT"), so they only rule out Person.
REGEX_BUSINESS_WORD = re.compile(
    r"\b(PHARMACY|PHCY|DRUGS?|TRANSPORT\w*|TRANS|"
    r"SERVICES?|SVCS?|SRVC?S?|CENTER|CTR|CLINIC|LAB\w*|CARE|HEALTH\w*|GROUP|"
    r"MEDICAL|MEDICAR|AMBULANCE|SUPPL\w*|EQUIPMENT|HOME|ASSOC\w*|ENTERPRISES?|"
    r"SOLUTIONS|CAB|TAXI|LIVERY|AGENCY|FOUNDATION|HOSPITAL|RX|DIAGNOSTICS?|"
    r"CONSULTANTS?|INSTITUTE|MANAGEMENT|SYSTEMS|THERAPY|WELLNESS|COUNSELING|"
    r"PSYCHIATRY)\b"
)
REGEX_DBA = re.compile(r"\s+D\.?/?B\.?/?A\b\.?/?\s*")
REGEX_NPI = re.compile(r"^\d{10}$")


def clean(value: str | None) -> str | None:
    if value is None:
        return None
    value = collapse_spaces(value)
    # A single row carries "?" in every address field.
    if value in ("", "?"):
        return None
    return value


def crawl_row(context: Context, row: dict[str, str | None]) -> None:
    raw_name = clean(row.pop("provider_name"))
    if raw_name is None:
        context.log.warning("Row without provider name", row=row)
        return
    # "PERSON DBA BUSINESS" is a sole proprietor: the trade name is the name,
    # the owner's name an alias.
    owner, *trade_names = REGEX_DBA.split(raw_name)
    name = trade_names[-1] if trade_names else owner
    aliases = [owner, *trade_names[:-1]] if trade_names else []
    npi = clean(row.pop("npi"))
    license = clean(row.pop("license"))
    provider_type = clean(row.pop("provider_type"))
    city = clean(row.pop("city"))

    if REGEX_LEGAL_FORM.search(raw_name):
        schema = "Company"
    elif trade_names or REGEX_BUSINESS_WORD.search(raw_name):
        schema = "LegalEntity"
    elif provider_type is None:
        # Untyped rows are mostly individuals barred for their role at a vendor,
        # but nothing in the row confirms it.
        schema = "LegalEntity"
    else:
        res = context.lookup("provider_type", provider_type, warn_unmatched=True)
        schema = "LegalEntity" if res is None or res.value is None else res.value

    entity = context.make(schema)
    if npi is not None:
        entity.id = context.make_id(raw_name, npi)
    else:
        entity.id = context.make_id(raw_name, license, city)
    # Persons are listed as "LAST FIRST MIDDLE", without a separator that would
    # allow splitting multi-word surnames reliably.
    entity.add("name", name)
    entity.add("alias", aliases)
    if npi is not None and not REGEX_NPI.match(npi):
        context.log.warning("Invalid NPI", npi=npi, name=raw_name)
        entity.add("description", f"NPI: {npi}")
    else:
        entity.add("npiCode", npi)
    entity.add("licenseNumber", license)
    entity.add("sector", provider_type)
    entity.add("topics", "debarment")
    entity.add("country", "us")
    address = h.make_address(
        context,
        street=clean(row.pop("address")),
        street2=clean(row.pop("address_2")),
        city=city,
        state=clean(row.pop("state")),
        postal_code=(clean(row.pop("zip_code")) or "").replace(" ", ""),
        country_code="us",
    )
    h.copy_address(entity, address)

    action_type = clean(row.pop("action_type"))
    action_date = row.pop("action_date")
    affiliation = clean(row.pop("affiliation"))
    provisions = context.lookup_value("action_type", action_type, warn_unmatched=True)
    # A barred person appears once per vendor they were affiliated with.
    sanction = h.make_sanction(
        context, entity, key=f"{action_date}-{action_type}-{affiliation}"
    )
    h.apply_date(sanction, "startDate", action_date)
    sanction.add("provisions", provisions)

    if affiliation is not None:
        vendor = context.make("Company")
        vendor.id = context.make_id("vendor", affiliation)
        vendor.add("name", affiliation)
        vendor.add("country", "us")
        link = context.make("UnknownLink")
        link.id = context.make_id(entity.id, vendor.id)
        link.add("subject", entity)
        link.add("object", vendor)
        link.add("role", "Affiliated vendor")
        context.emit(vendor)
        context.emit(link)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row)


def crawl(context: Context) -> None:
    # The Power Pages entity list is rendered client-side. Its view config is a
    # base64 JSON list in the `data-view-layouts` attribute; the export endpoint
    # takes the opaque `Base64SecureConfiguration` from it plus an anti-forgery
    # token bound to the session cookies set by the page request.
    doc = context.fetch_html(context.data_url)
    grid = h.xpath_element(doc, "//div[@data-view-layouts]")
    layouts_attr = grid.get("data-view-layouts")
    assert layouts_attr is not None
    layouts: list[dict[str, Any]] = json.loads(base64.b64decode(layouts_attr))
    # "Recent Additions" is a subset of "Active Sanctions".
    views = [layout["ViewName"] for layout in layouts]
    assert views == ["Active Sanctions", "Recent Additions"], views
    layout = layouts[0]
    [export] = [
        link
        for link in layout["Configuration"]["ViewActionLinks"]
        if link["Label"] == "Export Data"
    ]
    export_url = BASE_URL + export["URL"]["Path"]

    token_doc = context.fetch_html(BASE_URL + "/_layout/tokenhtml")
    token = h.xpath_string(
        token_doc, "//input[@name='__RequestVerificationToken']/@value"
    )
    # Page and pageSize are ignored: the workbook always holds the whole view.
    body = {
        "viewName": layout["ViewName"],
        "columns": layout["Columns"],
        "base64SecureConfiguration": layout["Base64SecureConfiguration"],
        "sortExpression": layout["SortExpression"],
        "search": None,
        "filter": None,
        "metaFilter": None,
        "page": 1,
        "pageSize": layout["Configuration"]["PageSize"],
        "timezoneOffset": 0,
    }
    resp = context.fetch_json(
        export_url,
        method="POST",
        data=json.dumps(body),
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "__RequestVerificationToken": token,
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    assert resp["success"] is True, resp
    # The session key is a one-shot handle on a server-side generated workbook.
    path = context.fetch_resource(
        "source.xlsx", f"{export_url}?key={resp['sessionKey']}"
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook = CalamineWorkbook.from_path(path)
    assert workbook.sheet_names == ["Active Sanctions"], workbook.sheet_names
    sheet = workbook.get_sheet_by_name("Active Sanctions")
    for row in h.parse_xlsx_sheet(context, sheet):
        crawl_row(context, row)
