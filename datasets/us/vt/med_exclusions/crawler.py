import re
from datetime import date
from urllib.parse import urljoin

from normality import squash_spaces
from python_calamine import CalamineWorkbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h
from zavod.entity import Entity

PORTAL_URL = "https://www.vtmedicaid.com/"
# The AngularJS view behind https://www.vtmedicaid.com/#/provEnrollExcluded
TEMPLATE_URL = "https://www.vtmedicaid.com/html/excludedProvidersList.html"
TITLE_PREFIX = "Vermont Monthly List of For Cause Exclusions"
# The sheet has two columns headed "State": the second holds the reason code.
HEADERS = [
    "medicaid_id",
    "npi",
    "name",
    "provider_type",
    "city",
    "state",
    "reason_code",
    "reason",
    "start_date",
    "end_date",
    "reinstatement_date",
]
SOURCE_HEADERS = [
    "VT Medicaid Provider ID",
    "Provider NPI",
    "Provider Full Name",
    "Provider Type",
    "Provider City",
    "State",
    "State",
    'Reason or "Cause" for Exclusion',
    "Provider Exclusion Status Effective Date",
    "Provider Exclusion Status End Date",
    "Reinstatement Date with Vermont Medicaid if Applicable",
]
REASON_CODES = {
    "07": "TERMED DUE TO MEDICARE/OTHER STATE EXCLUSION",
    "08": "TERMINATION DUE TO CONVICTION OF FRAUD",
    "19": "TERMINATED DUE TO FAILURE OF CONTRACT TERMS",
    "22": "TERMINATED DUE TO SANCTIONS",
}
INDEFINITE_END_DATE = "2382-12-31"
GENERATIONAL_SUFFIXES = {"JR", "SR", "II", "III", "IV"}
REGEX_NPI = re.compile(r"^\d{10}$")


def split_suffix(tokens: list[str]) -> tuple[list[str], str | None]:
    if len(tokens) > 1 and tokens[-1] in GENERATIONAL_SUFFIXES:
        return tokens[:-1], tokens[-1]
    return tokens, None


def apply_person_name(context: Context, entity: Entity, raw_name: str) -> None:
    """Apply a "LAST,FIRST MIDDLE CREDENTIAL" name to a person."""
    if raw_name.count(",") != 1:
        context.log.warning("Unexpected person name format", name=raw_name)
        entity.add("name", raw_name)
        return
    last, given = raw_name.split(",")
    last_tokens, last_suffix = split_suffix(last.split())
    given_clean = h.strip_name_titles(context, given)
    given_tokens, given_suffix = split_suffix((given_clean or "").split())
    if len(given_tokens) == 0 or len(given_tokens) > 2:
        context.log.warning("Unexpected given names", name=raw_name)
    h.apply_name(
        entity,
        first_name=given_tokens[0] if given_tokens else None,
        middle_name=" ".join(given_tokens[1:]) or None,
        last_name=" ".join(last_tokens),
        suffix=last_suffix or given_suffix,
    )


def crawl_row(context: Context, row: dict[str, str | None]) -> None:
    medicaid_id = row.pop("medicaid_id")
    raw_name = squash_spaces(row.pop("name") or "")
    assert medicaid_id is not None and raw_name != "", row
    npi = row.pop("npi")
    if npi is not None and REGEX_NPI.match(npi) is None:
        context.log.warning("Invalid NPI", npi=npi, name=raw_name)
        npi = None
    provider_type = squash_spaces(row.pop("provider_type") or "")

    schema = context.lookup_value("provider_type", provider_type)
    if schema is None:
        context.log.warning(
            "Unknown provider type", provider_type=provider_type, name=raw_name
        )
        schema = "LegalEntity"

    entity = context.make(schema)
    # One provider can hold several Vermont Medicaid IDs, but only one NPI.
    if npi is not None:
        entity.id = context.make_id("npi", npi)
    else:
        entity.id = context.make_id("vt-medicaid", medicaid_id)
    if schema == "Person":
        apply_person_name(context, entity, raw_name)
    else:
        entity.add("name", re.sub(r",(?=\S)", ", ", raw_name))
    entity.add("npiCode", npi)
    entity.add("description", f"Vermont Medicaid Provider ID: {medicaid_id}")
    entity.add("sector", provider_type)
    entity.add("country", "us")
    address = h.make_address(
        context,
        city=squash_spaces(row.pop("city") or ""),
        state=row.pop("state"),
        country_code="us",
    )
    h.copy_address(entity, address)

    reason_code = row.pop("reason_code")
    reason = squash_spaces(row.pop("reason") or "")
    if REASON_CODES.get(reason_code or "") != reason:
        context.log.warning(
            "Unexpected reason code", reason_code=reason_code, reason=reason
        )

    start_date = row.pop("start_date")
    sanction = h.make_sanction(context, entity, key=f"{medicaid_id}-{start_date}")
    h.apply_date(sanction, "startDate", start_date)
    sanction.add("reason", reason)
    end_date = row.pop("end_date")
    reinstatement_date = row.pop("reinstatement_date")
    if reinstatement_date is not None:
        h.apply_date(sanction, "endDate", reinstatement_date)
        sanction.add("status", "Reinstated")
    elif end_date != INDEFINITE_END_DATE:
        h.apply_date(sanction, "endDate", end_date)

    # The exclusion lasts until reinstatement, even past the end date of its term.
    reinstated = reinstatement_date is not None and reinstatement_date <= str(
        date.today()
    )
    if not reinstated:
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row)


def crawl(context: Context) -> None:
    template = context.fetch_html(TEMPLATE_URL)
    links = h.xpath_strings(template, "//a[contains(@href, '.xlsx')]/@href")
    if len(links) != 1:
        raise ValueError(f"Expected one spreadsheet link in template: {links}")
    # The template is rendered at the portal root, not at its own path.
    xlsx_url = urljoin(PORTAL_URL, links[0])
    if xlsx_url != context.data_url:
        context.log.warning("Spreadsheet URL changed", url=xlsx_url)

    path = context.fetch_resource("source.xlsx", xlsx_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook = CalamineWorkbook.from_path(path)
    assert workbook.sheet_names == ["Excluded Providers"], workbook.sheet_names
    sheet = workbook.get_sheet_by_name("Excluded Providers")
    rows = sheet.to_python(skip_empty_area=False)
    title = squash_spaces(" ".join(str(c) for c in rows[0]))
    if not title.replace("–", "-").startswith(TITLE_PREFIX):
        raise ValueError(f"Unexpected sheet title: {title}")
    headers = [squash_spaces(str(c)) for c in rows[1]]
    if headers != SOURCE_HEADERS:
        raise ValueError(f"Unexpected headers: {headers}")

    # Notes and a key follow the table, each in the first column only.
    in_table = True
    for row in h.parse_xlsx_sheet(context, sheet, skiprows=2, headers=HEADERS):
        is_note = all(v is None for k, v in row.items() if k != "medicaid_id")
        if is_note:
            in_table = False
            continue
        if not in_table:
            raise ValueError(f"Data row after the notes below the table: {row}")
        crawl_row(context, row)
