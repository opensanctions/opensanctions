import csv
import html
import json
import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from io import TextIOWrapper
from pathlib import Path
from typing import Any

from followthemoney.types import registry
from rigour.ids import EIN, LEI
from rigour.mime.types import ZIP

from zavod import Context, settings
from zavod import helpers as h

ENTITY_TYPES = {"operating", "investment", "other"}
ROLE_RE = re.compile(r"Director|Officer|TenPercentOwner|Other")
QUARTER_RE = re.compile(r"(\d{4})q([1-4])_form345\.zip$")
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


def crawl_filer(context: Context, data: dict[str, Any], cutoff: str) -> str | None:
    dates: list[str] = data["filings"]["recent"]["filingDate"]
    if len(dates) == 0 or max(dates) < cutoff:
        return None

    raw_cik: str = data.pop("cik")
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
    return cik


@dataclass
class Relation:
    """An insider role of one owner at one issuer, merged across all filings."""

    issuer: str
    owner: str
    kind: str
    date: str
    roles: set[str] = field(default_factory=set)
    remarks: set[str] = field(default_factory=set)


def read_tsv(zf: zipfile.ZipFile, name: str) -> Iterator[dict[str, str]]:
    with zf.open(name) as fh:
        text = TextIOWrapper(fh, encoding="utf-8")
        yield from csv.DictReader(text, delimiter="\t", quoting=csv.QUOTE_NONE)


def parse_roles(value: str) -> list[str]:
    # Usually comma-joined, but also concatenated, e.g. "TenPercentOwnerOther".
    roles = ROLE_RE.findall(value)
    if ROLE_RE.sub("", value).strip(", ") != "":
        raise ValueError(f"Unknown reporting owner relationship: {value!r}")
    return roles


def quarter_files(context: Context, cutoff: str) -> list[tuple[str, str]]:
    """List the Forms 3/4/5 quarterly ZIPs whose quarter ends after the cutoff."""
    # The URL path changed between quarters, so the links are read off the page.
    doc = context.fetch_html(
        "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets",
        absolute_links=True,
        cache_days=1,
    )
    files: dict[str, str] = {}
    for link in h.xpath_elements(doc, ".//a[contains(@href, '_form345.zip')]"):
        url = link.get("href")
        match = QUARTER_RE.search(url or "")
        if url is None or match is None:
            raise ValueError(f"Unexpected data set link: {url!r}")
        year, quarter = int(match.group(1)), int(match.group(2))
        next_start = f"{year + quarter // 4}-{(quarter % 4) * 3 + 1:02d}-01"
        if next_start > cutoff:
            files[match.group(0)] = url
    if len(files) == 0:
        raise ValueError("No Forms 3/4/5 data sets found")
    return sorted(files.items())


def crawl_insider_file(
    context: Context,
    path: Path,
    relations: dict[tuple[str, str, str], Relation],
    emitted: set[str],
    issuers: set[str],
) -> None:
    with zipfile.ZipFile(path, "r") as zf:
        filings: dict[str, tuple[str, str, str]] = {}
        for row in read_tsv(zf, "SUBMISSION.tsv"):
            issuer = row["ISSUERCIK"].zfill(10)
            date = datetime.strptime(row["FILING_DATE"], "%d-%b-%Y").date().isoformat()
            filings[row["ACCESSION_NUMBER"]] = (issuer, date, row["REMARKS"])
            if issuer not in issuers:
                # Section 16 issuers have listed equity. Emitting them as Company
                # also makes funds the register typed LegalEntity a valid
                # Ownership asset.
                company = context.make("Company")
                company.id = cik_id(issuer)
                company.add("name", row["ISSUERNAME"])
                company.add("cikCode", issuer)
                context.emit(company)
                issuers.add(issuer)
                emitted.add(issuer)

        for row in read_tsv(zf, "REPORTINGOWNER.tsv"):
            issuer, date, remarks = filings[row["ACCESSION_NUMBER"]]
            owner = row["RPTOWNERCIK"].zfill(10)
            if owner not in emitted:
                stub = context.make("LegalEntity")
                stub.id = cik_id(owner)
                stub.add("name", row["RPTOWNERNAME"])
                stub.add("cikCode", owner)
                context.emit(stub)
                emitted.add(owner)
            # Titles and remarks carry HTML entities such as &#8211;.
            title = html.unescape(row["RPTOWNER_TITLE"]).strip()
            remark = None
            if title.lower() == "see remarks":
                # Usually the title itself, sometimes with footnote prose appended.
                remark = html.unescape(remarks).strip()
                title = remark if len(remark) <= registry.string.max_length else ""
            for kind in parse_roles(row["RPTOWNER_RELATIONSHIP"]):
                key = (issuer, owner, kind)
                rel = relations.get(key)
                if rel is None:
                    rel = relations[key] = Relation(issuer, owner, kind, date)
                rel.date = max(rel.date, date)
                if kind == "Officer" and title:
                    rel.roles.add(title)
                if kind == "Officer" and remark:
                    rel.remarks.add(remark)
                elif kind == "Other" and row["RPTOWNER_TXT"]:
                    rel.roles.add(row["RPTOWNER_TXT"])


def emit_relation(context: Context, rel: Relation) -> None:
    if rel.kind in ("Director", "Officer"):
        schema, owner_prop, issuer_prop = "Directorship", "director", "organization"
    elif rel.kind == "TenPercentOwner":
        schema, owner_prop, issuer_prop = "Ownership", "owner", "asset"
    else:
        schema, owner_prop, issuer_prop = "UnknownLink", "subject", "object"
    entity = context.make(schema)
    entity.id = context.make_id(rel.issuer, rel.owner, rel.kind)
    entity.add(owner_prop, cik_id(rel.owner))
    entity.add(issuer_prop, cik_id(rel.issuer))
    entity.add("role", sorted(rel.roles) or rel.kind)
    entity.add("summary", sorted(rel.remarks))
    # Forms 3/4/5 do not state when a role began or ended; this is the latest filing.
    entity.add("date", rel.date)
    context.emit(entity)


def crawl(context: Context) -> None:
    path = context.fetch_resource("submissions.zip", context.data_url)
    context.export_resource(path, ZIP, title=context.SOURCE_TITLE)
    cutoff = h.backdate(settings.RUN_TIME, timedelta(days=5 * 365))
    emitted_ciks: set[str] = set()
    emitted = 0
    skipped = 0
    with zipfile.ZipFile(path, "r") as zf:
        for name in zf.namelist():
            # CIK##########-submissions-NNN.json hold older filing pages only.
            if not name.startswith("CIK") or "-submissions-" in name:
                continue
            data = json.loads(zf.read(name))
            cik = crawl_filer(context, data, cutoff)
            if cik is not None:
                emitted_ciks.add(cik)
                emitted += 1
            else:
                skipped += 1
            if (emitted + skipped) % 100_000 == 0:
                context.log.info("Processed filers", emitted=emitted, skipped=skipped)
    context.log.info("Filers without a recent filing", skipped=skipped)

    relations: dict[tuple[str, str, str], Relation] = {}
    issuers: set[str] = set()
    for file_name, url in quarter_files(context, cutoff):
        context.log.info("Parsing insider data set", file=file_name)
        file_path = context.fetch_resource(f"form345/{file_name}", url)
        crawl_insider_file(context, file_path, relations, emitted_ciks, issuers)
    for rel in relations.values():
        emit_relation(context, rel)
    context.log.info("Insider relations", relations=len(relations))
