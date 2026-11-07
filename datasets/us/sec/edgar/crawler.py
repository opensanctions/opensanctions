import csv
import html
import json
import re
import time
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from io import TextIOWrapper
from pathlib import Path
from typing import Any

from followthemoney.types import registry
from lxml import etree
from lxml import html as lxml_html
from normality import slugify
from requests.exceptions import ChunkedEncodingError, ConnectionError, RequestException
from rigour.ids import EIN, LEI
from rigour.mime.types import ZIP
from rigour.names import is_name

from zavod import Context, settings
from zavod import helpers as h

ENTITY_TYPES = {"operating", "investment", "other"}
ROLE_RE = re.compile(r"Director|Officer|TenPercentOwner|Other")
QUARTER_RE = re.compile(r"(\d{4})q([1-4])_form345\.zip$")
ANNUAL_FORMS = {"10-K", "10-K/A", "10-KT", "10-KT/A"}
ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
# Filings never change, so lookups are kept for longer than the 18-month window.
FILING_CACHE_DAYS = 600
# The SEC throttles sustained crawling well below its published request limit.
SEC_MIN_INTERVAL = 1.0
EX21_ROW = re.compile(r"^\s*21(\.\d+)?\b")
SUBSIDIARY_RE = re.compile(r"subsidiar", re.I)
NAME_HEAD = re.compile(r"\b(name|subsidiar|entity|entities)", re.I)
JUR_HEAD = re.compile(
    r"(jurisdiction|incorporat|organi[sz]|state|country|domicile|formation|laws of|location)",
    re.I,
)
PCT_HEAD = re.compile(r"(%|percent|owner|interest|held)", re.I)
PCT_CELL = re.compile(r"^(\d{1,3}(?:\.\d+)?)\s*%")
FOOTNOTE = re.compile(r"(?:\s*(?:\*+|\(\d{1,2}\)|\(\w\)|\d\)))+\s*$")
ENUM_CELL = re.compile(r"^(\(?\d{1,4}[.)]?|\(?[a-zA-Z][.)]|[●•·▪\-–*+†‡]+)$")
JUR_PREFIX = re.compile(
    r"^(an?\s+|the\s+)?((state|commonwealth|province|republic|kingdom)\s+of\s+)?",
    re.I,
)
# "incorporated on June 23, 1994 in TX", "a corporation of Delaware"
JUR_PHRASE = re.compile(
    r"\b(?:incorporated|organized|formed|chartered)\b.*?\bin\s+(?:the\s+)?(.+?)"
    r"(?:\s+and\b.*)?$|\b(?:corporation|company|partnership)\s+of\s+(.+)$",
    re.I,
)
# Separators between a name and its jurisdiction within a single cell.
NAME_PLACE_SEP = re.compile(r"\s*\(|,\s+|\s+[-–]\s+")
JUR_SUFFIX = re.compile(
    r"\s+(limited liability company|limited partnership|general partnership|"
    r"corporation|partnership|company|statutory trust|business trust|trust|"
    r"limited|llc|lp|corp\.?|inc\.?)\b.*$",
    re.I,
)
TRAILING_PAREN = re.compile(r"^(.*\S)\s*\(([^()]+)\)\s*$")
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
    # File paths differ between quarters, so the links are read off the page.
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


@dataclass
class AnnualReport:
    """A 10-K filing whose Exhibit 21 lists the filer's subsidiaries."""

    cik: str
    name: str
    accession: str
    date: str
    primary_document: str

    @property
    def base_url(self) -> str:
        return f"{ARCHIVES}/{int(self.cik)}/{self.accession.replace('-', '')}"


@dataclass
class SubsidiaryRow:
    name: str
    jurisdiction: str | None
    country: str | None
    percent: str | None


@dataclass
class Subsidiary:
    """A subsidiary relation of one filer, merged across all its EX-21 documents."""

    filer: str
    entity_id: str
    schema: str
    date: str
    percent: str | None
    urls: set[str] = field(default_factory=set)


class Pacer:
    """Space out requests to the SEC, following its fair-access policy."""

    def __init__(self) -> None:
        self.last = 0.0

    def wait(self) -> None:
        delay = SEC_MIN_INTERVAL - (time.monotonic() - self.last)
        if delay > 0:
            time.sleep(delay)
        self.last = time.monotonic()


def collect_reports(
    zf: zipfile.ZipFile, cik: str, name: str, filings: dict[str, Any], cutoff: str
) -> list[AnnualReport]:
    """List the filer's 10-Ks since the cutoff, including paginated older filings."""
    pages: list[dict[str, Any]] = [filings["recent"]]
    for page in filings["files"]:
        if page["filingTo"] >= cutoff:
            pages.append(json.loads(zf.read(page["name"])))
    reports: list[AnnualReport] = []
    for page in pages:
        rows = zip(
            page["form"],
            page["accessionNumber"],
            page["filingDate"],
            page["primaryDocument"],
        )
        for form, accession, date, document in rows:
            if form in ANNUAL_FORMS and date >= cutoff:
                reports.append(AnnualReport(cik, name, accession, date, document))
    return reports


def archive_link(href: str | None) -> str | None:
    """Normalise a link to an EDGAR archive document, dropping the iXBRL viewer."""
    if href is None or "sec.gov/Archives/edgar/data/" not in href:
        return None
    return href.replace("/ix?doc=", "/").replace("http://", "https://")


def cells_text(row: etree._Element) -> list[str]:
    texts = [h.element_text(c) for c in h.xpath_elements(row, "./td|./th")]
    return [t.replace("​", "").strip() for t in texts]


def find_referenced_ex21(context: Context, pacer: Pacer, url: str) -> list[str]:
    """Follow the exhibit index link of an EX-21 incorporated from an older filing."""
    pacer.wait()
    text = context.fetch_text(url)
    if text is None:
        raise ValueError(f"Empty filing document: {url}")
    doc = parse_html(text)
    urls: list[str] = []
    for row in h.xpath_elements(doc, ".//tr"):
        cells = [c for c in cells_text(row) if c]
        if len(cells) == 0 or EX21_ROW.match(cells[0]) is None:
            continue
        if SUBSIDIARY_RE.search(" ".join(cells)) is None:
            continue
        for link in h.xpath_elements(row, ".//a[@href]"):
            target = archive_link(link.get("href"))
            if target is not None and target not in urls:
                urls.append(target)
    return urls


def find_ex21_urls(context: Context, pacer: Pacer, report: AnnualReport) -> list[str]:
    key = f"ex21-urls:{report.accession}"
    cached = context.cache.get_json(key, max_age=FILING_CACHE_DAYS)
    if cached is not None:
        return [str(u) for u in cached]
    pacer.wait()
    index_url = f"{report.base_url}/{report.accession}-index.htm"
    doc = context.fetch_html(index_url, absolute_links=True)
    urls: list[str] = []
    for row in h.xpath_elements(doc, ".//table[@class='tableFile']//tr"):
        cells = h.xpath_elements(row, "./td")
        if len(cells) < 4 or not h.element_text(cells[3]).upper().startswith("EX-21"):
            continue
        for link in h.xpath_elements(cells[2], ".//a[@href]"):
            target = archive_link(link.get("href"))
            if target is not None and target not in urls:
                urls.append(target)
    if len(urls) == 0 and report.primary_document:
        primary_url = f"{report.base_url}/{report.primary_document}"
        urls = find_referenced_ex21(context, pacer, primary_url)
    context.cache.set_json(key, urls)
    return urls


def fetch_ex21(context: Context, pacer: Pacer, url: str) -> etree._Element:
    key = f"ex21-doc:{url}"
    text = context.cache.get(key, max_age=FILING_CACHE_DAYS)
    if text is None:
        pacer.wait()
        text = context.fetch_text(url)
        if text is None:
            raise ValueError(f"Empty EX-21 response: {url}")
        # Some exhibits contain NUL bytes, which PostgreSQL text fields reject.
        text = text.replace("\x00", "")
        context.cache.set(key, text)
    return parse_html(text)


def parse_html(text: str) -> etree._Element:
    # lxml refuses str input that carries an XML encoding declaration, which
    # inline XBRL documents start with.
    return lxml_html.fromstring(re.sub(r"^\s*<\?xml[^>]*\?>", "", text))


def resolve_place(context: Context, value: str) -> str | None:
    value = value.strip(" .,;:")
    if value == "":
        return None
    # Two-letter codes are US states: "DE" is Delaware, not Germany.
    if re.fullmatch(r"[A-Z]{2}|[A-Z]\.[A-Z]\.", value):
        res = context.lookup("edgar_location", value.replace(".", ""))
        return res.value if res is not None else None
    return registry.country.clean_text(value)


def resolve_jurisdiction(context: Context, value: str) -> str | None:
    """Resolve a jurisdiction cell, trying progressively trimmed variants of it."""
    # "Guangdong Province, China / November 29, 2016", "United States - DE"
    candidates = [value, value.split("/")[0], *value.split(" - ")]
    # "US (Delaware)"
    paren = TRAILING_PAREN.match(value)
    if paren is not None:
        candidates.extend(paren.groups())
    for cand in list(candidates):
        trimmed = JUR_SUFFIX.sub("", JUR_PREFIX.sub("", cand))
        candidates.append(trimmed)
        candidates.extend(trimmed.split(","))
    # "Connecticut captive insurance company"
    words = value.split()
    candidates.extend(" ".join(words[:n]) for n in (3, 2, 1) if len(words) > n)
    for cand in candidates:
        country = resolve_place(context, cand)
        if country is not None:
            return country
    return None


def compact_cells(row: etree._Element) -> list[str]:
    out: list[str] = []
    for cell in cells_text(row):
        if cell == "":
            continue
        # Tables split "100" and "%" (or "%(1)") into separate cells.
        if cell.startswith("%") and out and re.fullmatch(r"\d{1,3}(\.\d+)?", out[-1]):
            out[-1] += "%"
            continue
        out.append(cell)
    # Numbered or bulleted lists put the enumerator in its own cell.
    while len(out) > 1 and ENUM_CELL.match(out[0]):
        out.pop(0)
    return out


def detect_header(context: Context, cells: list[str]) -> dict[str, int] | None:
    columns: dict[str, int] = {}
    for idx, cell in enumerate(cells):
        # Data rows name companies and places; header rows do neither.
        if len(cell) > 80 or resolve_place(context, cell) is not None:
            return None
        if "name" not in columns and NAME_HEAD.search(cell):
            columns["name"] = idx
        elif "jur" not in columns and JUR_HEAD.search(cell):
            columns["jur"] = idx
        elif "pct" not in columns and PCT_HEAD.search(cell):
            columns["pct"] = idx
    if "name" in columns and ("jur" in columns or len(cells) <= 3):
        return columns
    return None


def clean_sub_name(value: str) -> str | None:
    name = FOOTNOTE.sub("", value).strip()
    if re.search(r"[^\W\d_]", name) is None:
        return None
    return name


def parse_row(
    context: Context, cells: list[str], header: dict[str, int] | None, width: int
) -> SubsidiaryRow | None:
    if header is not None and len(cells) == width:
        name = clean_sub_name(cells[header["name"]])
        jur = cells[header["jur"]] if "jur" in header else None
        pct = PCT_CELL.match(cells[header["pct"]]) if "pct" in header else None
        if name is not None and jur is None:
            # "PNNT Alabama Holdings Inc. (Delaware)"
            paren = TRAILING_PAREN.match(name)
            if paren is not None and resolve_jurisdiction(context, paren.group(2)):
                name, jur = paren.group(1), paren.group(2)
        country = resolve_jurisdiction(context, jur) if jur else None
        if name is not None and (jur is None or country is not None):
            return SubsidiaryRow(name, jur, country, pct.group(1) if pct else None)
    pcts = [m.group(1) for c in cells[1:] if (m := PCT_CELL.match(c))]
    pct_text = pcts[0] if pcts else None
    if len(cells) >= 2:
        # Without a matching header, the first cell is a name if another is a place.
        name = clean_sub_name(cells[0])
        for cell in cells[1:]:
            country = resolve_jurisdiction(context, cell)
            if country is not None and name is not None:
                return SubsidiaryRow(name, cell, country, pct_text)
    # "Forman Energy Corporation, a New York corporation", "Semtech EV, Inc. (California)"
    split = split_name_place(context, cells[0])
    if split is not None:
        name, jur, country = split
        return SubsidiaryRow(name, jur, country, pct_text)
    return None


def split_name_place(context: Context, text: str) -> tuple[str, str, str] | None:
    """Split a cell holding both a name and its jurisdiction, rightmost first."""
    text = text.strip(" .…")
    for match in reversed(list(NAME_PLACE_SEP.finditer(text))):
        rest = text[match.end() :]
        if "(" in match.group(0):
            # Only a parenthesis closing the cell: "Zoom (Hong Kong) LTD" is a name.
            if not rest.endswith(")"):
                continue
            rest = rest[:-1]
        if "(" in rest or ")" in rest:
            continue
        phrase = JUR_PHRASE.search(rest)
        if phrase is not None:
            rest = phrase.group(1) or phrase.group(2)
        country = resolve_jurisdiction(context, rest)
        # Drop dot leaders ("Jacobs Engineering Group Inc., …………") but keep "Inc.".
        head = re.sub(r"\s*(?:\.{2}|…)[\s.…]*$", "", text[: match.start()])
        name = clean_sub_name(head.strip(" ,"))
        if country is not None and name is not None:
            return name, rest, country
    return None


def parse_ex21(context: Context, doc: etree._Element) -> list[SubsidiaryRow]:
    """Read subsidiary rows from the tables of an EX-21 document."""
    rows: list[SubsidiaryRow] = []
    header: dict[str, int] | None = None
    width = 0
    # Tables split across pages repeat or drop the header, so it carries over.
    for tr in h.xpath_elements(doc, ".//tr"):
        cells = compact_cells(tr)
        if len(cells) == 0:
            continue
        found = detect_header(context, cells)
        if found is not None:
            header, width = found, len(cells)
            continue
        row = parse_row(context, cells, header, width)
        if row is not None:
            rows.append(row)
    return rows


def crawl_report(
    context: Context,
    pacer: Pacer,
    report: AnnualReport,
    subsidiaries: dict[tuple[str, str, str], Subsidiary],
) -> None:
    filer_slug = slugify(report.name, sep="")
    try:
        urls = find_ex21_urls(context, pacer, report)
    except RequestException as exc:
        # Uncached, so the next run retries this filing.
        context.log.warning("Cannot read filing", url=report.base_url, error=str(exc))
        return
    except (ValueError, etree.LxmlError) as exc:
        context.log.error("Cannot parse filing", url=report.base_url, error=str(exc))
        return
    for url in urls:
        try:
            doc = fetch_ex21(context, pacer, url)
        except RequestException as exc:
            context.log.warning("Cannot fetch EX-21", url=url, error=str(exc))
            continue
        except (ValueError, etree.LxmlError) as exc:
            context.log.error("Cannot parse EX-21", url=url, error=str(exc))
            continue
        rows = parse_ex21(context, doc)
        if len(rows) == 0:
            context.log.info("No subsidiary table in EX-21", url=url)
        for row in rows:
            if len(row.name) > 250 or not is_name(row.name):
                context.log.warning("Rejected subsidiary name", name=row.name, url=url)
                continue
            # Registrants often list themselves at the top of the exhibit.
            if slugify(row.name, sep="") == filer_slug:
                continue
            company = context.make("Company")
            company.id = sub_id = context.make_slug(
                report.cik, context.make_id(row.name, row.country)
            )
            if sub_id is None:
                raise ValueError(f"Cannot build subsidiary ID: {row!r}")
            company.add("name", row.name)
            company.add("jurisdiction", row.country)
            context.emit(company)
            # EX-21 lists entities the registrant controls, directly or not; a
            # stated percentage is an ownership share.
            schema = "Ownership" if row.percent is not None else "Control"
            key = (report.cik, sub_id, schema)
            sub = subsidiaries.get(key)
            if sub is None:
                sub = Subsidiary(report.cik, sub_id, schema, report.date, row.percent)
                subsidiaries[key] = sub
            if report.date >= sub.date:
                sub.date, sub.percent = report.date, row.percent or sub.percent
            sub.urls.add(url)


def emit_subsidiary(context: Context, sub: Subsidiary) -> None:
    entity = context.make(sub.schema)
    entity.id = context.make_id("ex21", sub.schema, sub.filer, sub.entity_id)
    if sub.schema == "Ownership":
        entity.add("owner", cik_id(sub.filer))
        entity.add("asset", sub.entity_id)
        entity.add("percentage", sub.percent)
    else:
        entity.add("controller", cik_id(sub.filer))
        entity.add("controlled", sub.entity_id)
    # The latest annual report listing the subsidiary, not a start of control.
    entity.add("date", sub.date)
    entity.add("sourceUrl", sorted(sub.urls))
    context.emit(entity)


def fetch_bulk(context: Context, name: str, url: str) -> Path:
    """Download a large file, resuming when the server drops the connection."""
    path = context.get_resource_path(name)
    # Reuse a completed download, as fetch_resource does.
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.part")
    partial.unlink(missing_ok=True)
    total: int | None = None
    for _ in range(10):
        offset = partial.stat().st_size if partial.exists() else 0
        headers = {"Range": f"bytes={offset}-"} if offset else {}
        try:
            with context.http.get(url, headers=headers, stream=True) as res:
                res.raise_for_status()
                if offset and res.status_code != 206:
                    raise ValueError(f"Server ignored range request: {url}")
                if total is None:
                    total = int(res.headers["Content-Length"])
                with open(partial, "ab") as fh:
                    for chunk in res.iter_content(chunk_size=1 << 20):
                        fh.write(chunk)
        except (ChunkedEncodingError, ConnectionError) as exc:
            size = partial.stat().st_size if partial.exists() else 0
            context.log.info(
                "Download interrupted, resuming", size=size, error=str(exc)
            )
            continue
        if partial.stat().st_size != total:
            raise ValueError(f"Incomplete download: {url}")
        partial.replace(path)
        return path
    raise RuntimeError(f"Download kept failing: {url}")


def crawl(context: Context) -> None:
    path = fetch_bulk(context, "submissions.zip", context.data_url)
    context.export_resource(path, ZIP, title=context.SOURCE_TITLE)
    cutoff = h.backdate(settings.RUN_TIME, timedelta(days=5 * 365))
    ex21_cutoff = h.backdate(settings.RUN_TIME, timedelta(days=548))
    emitted_ciks: set[str] = set()
    reports: list[AnnualReport] = []
    emitted = 0
    skipped = 0
    with zipfile.ZipFile(path, "r") as zf:
        for name in zf.namelist():
            # CIK##########-submissions-NNN.json hold older filing pages only.
            if not name.startswith("CIK") or "-submissions-" in name:
                continue
            data = json.loads(zf.read(name))
            filings: dict[str, Any] = data["filings"]
            filer_name: str = data["name"]
            cik = crawl_filer(context, data, cutoff)
            if cik is not None:
                emitted_ciks.add(cik)
                emitted += 1
                reports.extend(
                    collect_reports(zf, cik, filer_name, filings, ex21_cutoff)
                )
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

    pacer = Pacer()
    subsidiaries: dict[tuple[str, str, str], Subsidiary] = {}
    context.log.info("Annual reports to scan for EX-21", reports=len(reports))
    for idx, report in enumerate(reports):
        crawl_report(context, pacer, report, subsidiaries)
        if idx and idx % 500 == 0:
            context.log.info("Scanned annual reports", reports=idx)
    for sub in subsidiaries.values():
        emit_subsidiary(context, sub)
    context.log.info("Subsidiary relations", relations=len(subsidiaries))
