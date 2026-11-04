import json
import re
import time
from typing import Any
from urllib.parse import urlencode

from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.extract.zyte_api import ZyteAPIRequest, ZyteResult

BASE_URL = "http://bizreg.esrpska.com"
# Per-company responses are reused for a little less than the monthly crawl
# interval, so that an interrupted run can resume without paying for them again.
CACHE_DAYS = 25
# The register answers some requests with a transient HTTP 500 that succeeds on
# retry. Zyte itself only retries its own errors.
ATTEMPTS = 3
# Municipality IDs in use lie between 1 and 175.
MAX_MUNICIPALITY_ID = 200
# "60-01-0004-17 (1-5205-00)": the current MBS, then the entry in the old court
# register.
REGEX_MBS = re.compile(r"^(\S+)\s+\((\S+)\)$")

# Listing fields the register always leaves empty; the values are on the detail page.
EMPTY_LISTING_FIELDS = [
    "OdgovornoLice",
    "SkracenoPoslovnoIme",
    "MBS",
    "MB",
    "PIB",
    "Fax",
    "ObavjestenjaOStatususu",
]


def fetch(
    context: Context, path: str, form: dict[str, str] | None, cache_days: int | None
) -> ZyteResult:
    """Fetch a register page through Zyte, POSTing the form if one is given."""
    # Without any custom header, Zyte takes about a minute to return a page from
    # this register; with one, a second or two.
    request = ZyteAPIRequest(
        url=f"{BASE_URL}{path}", geolocation="BA", headers={"Accept": "text/html"}
    )
    if form is not None:
        request.method = "POST"
        request.body = urlencode(form).encode("utf-8")
        request.headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
        }
    for attempt in range(1, ATTEMPTS + 1):
        result = zyte_api.fetch(context, request, cache_days=cache_days)
        if result.status_code in (None, 200):
            return result
        context.log.info(
            "Register request failed", url=request.url, status=result.status_code
        )
        time.sleep(10 * attempt)
    raise RuntimeError(f"Register request failed {ATTEMPTS} times: {request.url}")


def fetch_records(
    context: Context, path: str, form: dict[str, str], cache_days: int | None
) -> tuple[list[dict[str, Any]], int]:
    """Fetch one page of a jTable list endpoint, with the total record count."""
    result = fetch(context, path, form, cache_days)
    try:
        data = json.loads(result.response_text)
    except json.JSONDecodeError:
        result.invalidate_cache(context)
        raise
    if data["Result"] != "OK":
        raise ValueError(f"jTable error for {path}: {data}")
    if not result.from_cache and cache_days is not None:
        context.cache.set(result.cache_fingerprint, result.response_text)
    # An empty result comes back as {} rather than [].
    records: list[dict[str, Any]] = list(data["Records"] or [])
    return records, data["TotalRecordCount"]


def fetch_details(context: Context, company_pid: int) -> dict[str, str]:
    """Fetch the basic data tab of a company's detail page as label/value pairs."""
    path = f"/Home/PregledPoslovnogSubjekta/{company_pid}"
    result = fetch(context, path, None, CACHE_DAYS)
    doc = html.fromstring(result.response_text)
    groups = h.xpath_elements(
        doc,
        '//div[@id="osnovniPodaci"]//div[contains(@class, "form-group")]'
        '[label[contains(@class, "control-label")]]',
    )
    details: dict[str, str] = {}
    for group in groups:
        label = h.xpath_element(group, './label[contains(@class, "control-label")]')
        value = h.xpath_element(group, './/label[contains(@class, "text-info")]')
        details[h.element_text(label)] = h.element_text(value)
    if "ЈИБ" not in details:
        raise ValueError(f"No basic data on detail page: {path}")
    if not result.from_cache:
        context.cache.set(result.cache_fingerprint, result.response_text)
    return details


def emit_party(context: Context, schema: str, company_id: str, name: str) -> str | None:
    """Emit a founder, representative or beneficial owner of a company.

    The same name within one company yields the same entity, so a founder who is
    also a representative or beneficial owner is a single party.
    """
    party = context.make(schema)
    party.id = context.make_id("party", company_id, name)
    party.add("name", name)
    if not party.has("name"):
        context.log.info("Skipping party without a usable name", name=name)
        return None
    context.emit(party)
    return party.id


def crawl_founders(context: Context, company_id: str, founders: str | None) -> None:
    # Founders are joined as "Mahmut Mujkić,<br>Meliha Mujkić", and may be people
    # or legal entities.
    if founders is None:
        return
    for name in founders.split("<br>"):
        # Trimmed before it keys the IDs: the separator leaves a comma and spaces
        # on the name, and the trimmed name is what lets a founder merge with a
        # representative or beneficial owner of the same name.
        name = name.strip().removesuffix(",").strip()
        if name == "":
            continue
        party_id = emit_party(context, "LegalEntity", company_id, name)
        if party_id is None:
            continue
        own = context.make("Ownership")
        own.id = context.make_id("founder", company_id, name)
        own.add("asset", company_id)
        own.add("owner", party_id)
        own.add("role", "Founder", lang="eng")
        context.emit(own)


def crawl_representatives(context: Context, company_id: str, pid: int) -> None:
    path = f"/Home/ListLicaOvlastenaZaZastupanjePrivrednogDrustva/{pid}"
    # The endpoint rejects an empty POST body; it does not page.
    form = {"jtStartIndex": "0"}
    records, _ = fetch_records(context, path, form, CACHE_DAYS)
    for record in records:
        name = record.pop("ImeIPrezime")
        role = record.pop("Polozaj")
        party_id = emit_party(context, "Person", company_id, name)
        if party_id is None:
            continue
        rel = context.make("Directorship")
        rel.id = context.make_id("directorship", company_id, name, role)
        rel.add("director", party_id)
        rel.add("organization", company_id)
        rel.add("role", role)
        rel.add("description", record.pop("ObimOvlastenja"))
        context.emit(rel)
        context.audit_data(record, ignore=["ZastupnikId", "PrivrednoDrustvoId"])


def crawl_beneficial_owners(
    context: Context, company_id: str, pid: int, jib: str
) -> None:
    path = "/Vlasnik/PretragaList?jtStartIndex=0&jtPageSize=100"
    form = {"Ime": "", "Prezime": "", "PoslovniSubjekatSearchTerm": jib}
    records, total = fetch_records(context, path, form, CACHE_DAYS)
    if total > len(records):
        raise ValueError(f"More than one page of beneficial owners for {jib}")
    for record in records:
        # The search matches the JIB as free text, so it can return other companies.
        if record.pop("PrivrednoDrustvoId") != pid:
            continue
        name = record.pop("PunoIme")
        share = record.pop("Udio")
        party_id = emit_party(context, "Person", company_id, name)
        if party_id is None:
            continue
        own = context.make("Ownership")
        own.id = context.make_id("beneficial", company_id, name)
        own.add("asset", company_id)
        own.add("owner", party_id)
        own.add("role", "Beneficial owner", lang="eng")
        # The share is a percentage, but a few entries hold what looks like an
        # amount of capital instead.
        if share is not None and share <= 100:
            own.add("percentage", str(share))
        elif share is not None:
            context.log.info(
                "Beneficial owner share is not a percentage",
                company=company_id,
                share=share,
            )
        context.emit(own)
        context.audit_data(record, ignore=["PrivrednoDrustvoNaziv"])


def crawl_company(context: Context, record: dict[str, Any]) -> None:
    pid: int = record.pop("PrivrednoDrustvoId")
    # The first line is the status, the following lines date the steps of any
    # bankruptcy or liquidation proceedings.
    status = record.pop("StatusPoslovniSubjekatOpis").split("<br>")[0].strip()
    res = context.lookup_value("status", status, warn_unmatched=True)
    if res != "active":
        return

    company = context.make("Company")
    company.id = context.make_slug(str(pid))
    company.add("name", record.pop("PoslovnoIme"))
    company.add("country", "ba")
    company.add("status", status)
    address = record.pop("Sjediste")
    if address is not None:
        # " , Banja Luka, Banja Luka" when there is no street.
        parts = [p.strip() for p in address.split(",")]
        company.add("address", ", ".join(p for p in parts if p != ""))
    company.add("sector", record.pop("PreteznaDjelatnost"))
    company.add("phone", record.pop("Telefon"))
    company.add("email", record.pop("Email"))
    jib: str | None = record.pop("JIB")
    company.add("taxNumber", jib)
    company.add("sourceUrl", f"{BASE_URL}/Home/PregledPoslovnogSubjekta/{pid}")

    details = fetch_details(context, pid)
    if details.pop("ЈИБ") != (jib or ""):
        raise ValueError(f"Detail page of {pid} shows a different JIB")
    company.add("alias", details.pop("Скраћено пословно име"))
    mbs = details.pop("МБС / Рег.уложак")
    if match := REGEX_MBS.match(mbs):
        company.add("registrationNumber", match.groups())
    else:
        company.add("registrationNumber", mbs)
    company.add("registrationNumber", details.pop("МБ"))
    context.audit_data(
        details,
        # Repeats the listing, or out of scope (customs number, fax).
        ignore=[
            "Пословно име",
            "Сједиште",
            "Емаил",
            "Факс",
            "Телефон",
            "Царински број",
            "Статус пословног субјекта",
        ],
    )
    context.emit(company)
    assert company.id is not None

    crawl_founders(context, company.id, record.pop("Osnivaci"))
    crawl_representatives(context, company.id, pid)
    if jib is not None:
        crawl_beneficial_owners(context, company.id, pid, jib)

    for field in EMPTY_LISTING_FIELDS:
        if record.pop(field) is not None:
            context.log.warning("Listing field is no longer empty", field=field)
    context.audit_data(record, ignore=["PredmetId", "PoslovneJedinice"])


def crawl_municipality(context: Context, municipality_id: int) -> int:
    """Crawl all companies seated in one municipality, returning their count."""
    form = {
        "term": "",
        "opstinaId": str(municipality_id),
        "osnivac": "",
        "djelatnostId": "0",
    }
    offset = 0
    while True:
        query = urlencode(
            {
                "jtStartIndex": offset,
                "jtPageSize": 500,
                "jtSorting": "PoslovnoIme ASC",
            }
        )
        path = f"/Home/SearchPoslovniSubjekt?{query}"
        records, total = fetch_records(context, path, form, None)
        for record in records:
            crawl_company(context, record)
        offset += len(records)
        if offset >= total:
            return total
        if len(records) == 0:
            raise ValueError(
                f"Municipality {municipality_id} listing ended at {offset} of {total}"
            )


def crawl(context: Context) -> None:
    # Municipality IDs are internal to the register, so probe the whole range;
    # an unused ID lists no companies.
    used: list[int] = []
    for municipality_id in range(1, MAX_MUNICIPALITY_ID + 1):
        total = crawl_municipality(context, municipality_id)
        context.log.info("Crawled municipality", id=municipality_id, total=total)
        if total > 0:
            used.append(municipality_id)
    if len(used) < 70:
        raise ValueError(f"Only {len(used)} municipalities list any companies")
    if max(used) > MAX_MUNICIPALITY_ID - 10:
        context.log.warning(
            "Municipality IDs approach the end of the probed range",
            max_id=max(used),
        )
