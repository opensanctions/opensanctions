import json
import zipfile
from datetime import timedelta
from typing import Any

from followthemoney.types import registry
from rigour.ids import EIN, LEI
from rigour.mime.types import ZIP

from zavod import Context, settings
from zavod import helpers as h

ENTITY_TYPES = {"operating", "investment", "other"}
IGNORE = [
    "filings",
    "category",
    "description",
    "exchanges",
    "fiscalYearEnd",
    "flags",
    "insiderTransactionForIssuerExists",
    "insiderTransactionForOwnerExists",
    "investorWebsite",
    "ownerOrg",
    "phone",
    "tickers",
    "website",
]


def cik_id(cik: str) -> str:
    """Build the dataset-independent entity ID for a 10-digit, zero-padded CIK."""
    return f"us-sec-cik-{cik}"


def resolve_location(
    context: Context, code: str | None, desc: str | None
) -> str | None:
    """Map an EDGAR state or country code to a country code."""
    if code is None or code == "":
        return None
    res = context.lookup("edgar_location", code.upper())
    if res is not None:
        return res.value
    country = registry.country.clean_text(desc) if desc is not None else None
    if country is None:
        context.log.warning("Unknown EDGAR location code", code=code, description=desc)
    return country


def make_address(context: Context, addr: dict[str, Any] | None) -> str | None:
    if addr is None:
        return None
    street = addr.pop("street1")
    street2 = addr.pop("street2")
    city = addr.pop("city")
    postal_code = addr.pop("zipCode")
    code = addr.pop("stateOrCountry")
    desc = addr.pop("stateOrCountryDescription")
    foreign_state = addr.pop("foreignStateTerritory")
    context.audit_data(addr, ignore=["isForeignLocation", "country", "countryCode"])
    if street is None and city is None:
        return None
    country_code = resolve_location(context, code, desc)
    state = code.upper() if country_code == "us" and code else foreign_state
    return h.format_address(
        street=street,
        street2=street2,
        city=city,
        postal_code=postal_code,
        state=state,
        country_code=country_code,
    )


def crawl_filer(context: Context, data: dict[str, Any], cutoff: str) -> bool:
    dates: list[str] = data["filings"]["recent"]["filingDate"]
    if len(dates) == 0 or max(dates) < cutoff:
        return False

    raw_cik = data.pop("cik")
    if not raw_cik.isdigit() or len(raw_cik) > 10:
        raise ValueError(f"Invalid CIK: {raw_cik!r}")
    cik = raw_cik.zfill(10)
    entity_type = data.pop("entityType")
    if entity_type not in ENTITY_TYPES:
        raise ValueError(f"Unknown entity type {entity_type!r} for CIK {cik}")

    raw_ein = data.pop("ein")
    # Foreign filers and individuals carry placeholders like 000000000.
    ein = EIN.normalize(raw_ein) if raw_ein else None
    sic = data.pop("sic") or None
    jurisdiction = resolve_location(
        context,
        data.pop("stateOfIncorporation") or None,
        data.pop("stateOfIncorporationDescription") or None,
    )
    has_company_signal = ein is not None or sic is not None or jurisdiction is not None
    if entity_type == "operating" or (entity_type == "other" and has_company_signal):
        schema = "Company"
    else:
        schema = "LegalEntity"

    entity = context.make(schema)
    entity.id = cik_id(cik)
    entity.add("name", data.pop("name"))
    entity.add("cikCode", cik)
    entity.add("taxNumber", ein)
    entity.add("jurisdiction", jurisdiction)
    entity.add("sector", data.pop("sicDescription") or None)
    # Free-text field: holds phone numbers, fund names and "N/A" as often as LEIs.
    raw_lei = data.pop("lei")
    entity.add("leiCode", LEI.normalize(raw_lei) if raw_lei else None)
    for former in data.pop("formerNames"):
        entity.add("previousName", former["name"])
    addresses: dict[str, Any] = data.pop("addresses")
    for kind in ("business", "mailing"):
        entity.add("address", make_address(context, addresses.pop(kind, None)))
    context.audit_data(addresses)
    context.audit_data(data, ignore=IGNORE)
    context.emit(entity)
    return True


def crawl(context: Context) -> None:
    path = context.fetch_resource("submissions.zip", context.data_url)
    context.export_resource(path, ZIP, title=context.SOURCE_TITLE)
    cutoff = h.backdate(settings.RUN_TIME, timedelta(days=5 * 365))
    emitted = 0
    skipped = 0
    with zipfile.ZipFile(path, "r") as zf:
        for name in zf.namelist():
            # CIK##########-submissions-NNN.json hold older filing pages only.
            if not name.startswith("CIK") or "-submissions-" in name:
                continue
            data = json.loads(zf.read(name))
            if crawl_filer(context, data, cutoff):
                emitted += 1
            else:
                skipped += 1
            if (emitted + skipped) % 100_000 == 0:
                context.log.info("Processed filers", emitted=emitted, skipped=skipped)
    context.log.info("Filers without a recent filing", skipped=skipped)
