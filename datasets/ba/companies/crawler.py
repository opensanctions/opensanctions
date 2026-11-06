import datetime
import re
import time
from dataclasses import dataclass, field, replace
from typing import Any
from urllib.parse import urljoin

import requests
from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h

# Known failure mode: detail links the registry itself emits broken.
#
# The listing page builds each detail link by URL-encoding the company name into
# the APEX `f?p=` argument list, but its encoder stops at the first colon in the
# name and emits the remainder raw, e.g. for
# 'Društvo za proizvodnju, trgovinu, usluge i zastupanje "TEPIH SAN" d.o.o: Busovača':
#   f?p=186:13:<session>::NO:13:P13_P_POS_ID,P13_XMBS,P13_NAZIV:21054781%2C51-01-0090-08%2C%5CDru%C5%A1tvo%20...%20d.o.o: Busovača\&cs=3BB92596...
# `f?p=` is itself colon-delimited, so the raw colon starts a new argument, the
# name arrives truncated and APEX answers with a "Session state protection
# violation: This may be caused by manual alteration of a URL containing a
# checksum" page. The `&cs=` checksum is generated server-side over the intact
# name, and it is mandatory: dropping it, re-encoding the colon as %3A, quoting
# the whole argument or POSTing `p`/`cs` all produce the same violation. These
# links therefore cannot be repaired from our side, and are logged at info level
# rather than as warnings. The affected companies are still emitted from their
# listing row, but without legal form, status, JIB, founders or managers.
# 459 of 53,382 records in the 2026-08-21 run.
#
# Separately, a handful of names containing "&" are rejected before they reach
# APEX by the registry's web application firewall, which answers
# "The requested URL was rejected. [...] Your support ID is: ..." for the
# fully-encoded URL over both GET and POST, while other "&" names go through
# untouched (7 records in the same run). That is also outside our control, so
# it is logged at info level, but counted with the run's errors so that a rise
# in rejections fails the run.
WAF_REJECTION = "The requested URL was rejected"

# Companies emitted from their listing row alone because their pages failed
# verification even on a fresh session.
EXPECTED_ERRORS = 100

# A well-formed detail link carries eight colon-delimited APEX arguments - app,
# page, session, request, debug, clear cache, item names, item values - and
# therefore seven colons. Any extra colon comes from an unescaped one in the
# company name.
APEX_ARG_COLONS = 7

# The listing never returns more than this many rows, so a slice that reaches it
# has been truncated.
LISTING_CAP = 500
# Codes of the legal form filter: d.o.o., d.d., k.d., d.n.s.o., ob/pred, other
LEGAL_FORMS = ["1", "2", "3", "4", "6", "5"]

BASE_URL = "https://bizreg.pravosudje.ba/pls/apex/"
# Endpoint for the APEX application processes that set up a search
PROCESS_URL = f"{BASE_URL}wwv_flow.show"
# Endpoint for rendering a page, or one report region of it
PAGE_URL = f"{BASE_URL}f"
# The founders (21) and managers (22) pages carry no company identifier: they
# show whichever company the session last opened on the detail page (13).
FOUNDERS_PAGE = 21
MANAGERS_PAGE = 22

# Matično registarski broj (MBS), the court register number in the listing, as
# in "65-01-0119-23" or "1-638-00"
REGEX_MBS = re.compile(r"^\d+(?:-\d+)+$")
# Jedinstveni identifikacioni broj (JIB), the tax and statistics ID
REGEX_JIB = re.compile(r"^\d{13}$")
REGEX_COMPANY_KEY = re.compile(r"P13_NAZIV:(\d+)%2C")
REGEX_REPORT_PAGE = re.compile(r"\$a_report\('(\d+)','(\d+)','(\d+)','(\d+)'\)")
# A founder's registration number followed by its old number, as in
# "51-01-0812-09 (stari broj 1-4819)" or "65-01-0198-12 (1-7422)"
REGEX_OLD_REGNO = re.compile(
    r"^(.*?)\s*\((?:stari broj\s*:?)?\s*([^()]+)\)\s*$", re.IGNORECASE
)
# Labels written in front of a founder's numbers, as in "MBS: 51-01-0081-12" or
# "65-01-0045-24  JIB: 4203286650000"
REGEX_REGNO_LABEL = re.compile(r"\b(?:MBS|JIB|PIB|ID(?: broj)?)\s*:\s*")
# The text "Naprijed" ("Next") also labels an image in the same cell.
NEXT_PAGE_XPATH = '//a[@class="fielddata" and normalize-space()="Naprijed"]/@href'

# The founder table of a formerly state-owned company routinely carries a note
# about the collective of shareholders ("dioničari") or about where their list is
# kept, instead of the name of a founding entity, e.g. "Dioničari prema listi
# dioničara Registra vrijednosnih papira FBiH" or plain "Dioničari". Every one of
# the 142 distinct values seen in the 2026-08-28 run was such a note and none of
# them named an entity, so they are skipped rather than emitted as a founder.
SHAREHOLDER_NOTE = "dioničari"

FOUNDER_DENY_LIST = {
    "-",
    "Dioničari prema evidenciji Registra vrijednosnih papir F BiH",
    "Dioničari prema knjizi Dioničara",
    "dioničari prema listi u prilogu",
    "prema listi  dioničari",
    "prema listi dioničari",
}

# MBS	12-34-5678-90	2 digits "-" 2 digits "-" 4 digits "-" 2 digits
# MBS	1-2345-67	1 digit "-" 4 digits "-" 2 digits
# MBS	1-123	1 digit "-" 3 digits
# MBS	1-1234	1 digit "-" 4 digits
# MBS	1-12345	1 digit "-" 5 digits
# JIB	1234567890123	13 digits
# PIB	123456789012	12 digits
REGEX_ROUGH_REGNO = re.compile(r"^\d+-?\d{2,}-?[\d-]+$")
REMOVE_PATTERNS = [
    r"^[\"(]?BRISAN iz sudskog registra[:)\"]?",
    r"^[\"(]?BRISAN(?: USLJED PRIPAJANJA)?[:)\"]?",
    r"- BRISAN \w+$",  # "Deleted" and one word
    r"^\"BRISAN ZBOG ZAKLJUČENJA LIKVIDACIJE[:)\"]?",
    r"\(PRESTANAK\),? u likvidaciji",
    r"\(u LIKVIDACIJI\)",
    r"\(?U STEČAJU[:)\"]?$",
]
SPLITS = [
    "(skraćeni naziv:",
    ", skraćeni naziv:",
    "skraćena oznaka firme:",
    "skraćeno:",
    "LOCAL COMMUNITY",
    "LOCAL COMUNITY",  # their typo - usually a translation, but I think it works to just split.
]
REMOVE_REGEX = re.compile("|".join(REMOVE_PATTERNS), flags=re.IGNORECASE)

Record = dict[str, str]


class SessionLost(Exception):
    """The registry stopped serving the pages of the company that was opened."""


@dataclass(frozen=True)
class Search:
    """One query of the registry's advanced search.

    The "-1" and "" defaults leave a filter out. Each yes/no filter takes "1" or
    "0", and the two values between them cover every company.
    """

    city: str
    legal_form: str = "-1"
    removed: str = "-1"
    bankrupt: str = "-1"
    foreign_trade: str = "-1"
    registered_from: str = ""
    registered_to: str = ""


@dataclass
class CrawlState:
    """The APEX session all requests run in, and how often it had to be rescued."""

    instance: str
    renewals: int = 0
    errors: int = 0
    # A search that listed companies earlier in the run
    canary: Search | None = None
    # Companies emitted with their pages, and how many of them without founders
    companies: int = 0
    without_founders: int = 0
    # Companies whose founders page listed nobody, held back to be fetched again,
    # by registry company ID: the search listing them, the listing row and pages
    deferred: dict[str, tuple[Search, dict[str, str | None], dict[str, Any]]] = field(
        default_factory=dict
    )


def roughly_valid_regno(regno: str) -> bool:
    """
    Check if the registration number is valid
    Args:
        regno: The registration number to check.
    Returns:
        True if the registration number is valid, False otherwise.
    """
    return bool(REGEX_ROUGH_REGNO.match(regno))


def apex_url(url: str) -> str:
    """Percent-encode ampersands inside the APEX `f?p=` parameter value.

    The registry embeds the company name in the `p=` parameter, so a name
    containing "&" splits the query string when the URL is re-encoded on its
    way to the server, and the request comes back as a 400.
    """
    prefix, sep, checksum = url.rpartition("&cs=")
    if not sep:
        return url
    return f"{prefix.replace('&', '%26')}{sep}{checksum}"


def apex_link_broken(url: str) -> bool:
    """Check whether the registry left an unescaped colon in a detail link.

    The company name is the last APEX argument, so a colon the registry failed to
    encode adds an argument to the `f?p=` list. Such a link always fails the
    registry's session state protection check and cannot be repaired; see the note
    at the top of this file.

    Args:
        url: The details URL taken from the listing page.
    Returns:
        True if the link carries an unescaped colon, False otherwise.
    """
    prefix, _, _ = url.rpartition("&cs=")
    args = (prefix or url).partition("f?p=")[2]
    return args.count(":") > APEX_ARG_COLONS


def company_key(details_url: str) -> str:
    """Return the registry's internal ID of a company from its detail link."""
    match = REGEX_COMPANY_KEY.search(details_url)
    assert match is not None, details_url
    return match.group(1)


def comparable_regno(value: str) -> str:
    """Normalise a registration number for checking a page against the listing.

    The listing writes a missing number as "-", and the pages as an empty or
    non-breaking space value. Whitespace is collapsed, because the page text
    is read that way.
    """
    value = " ".join(value.replace("\xa0", " ").split())
    return "" if value == "-" else value


def open_session(context: Context) -> str:
    """
    Goes through the chain of redirects to get the APEX session instance.

    The whole crawl hangs off this session identifier, so a failure here is
    structural: it is raised rather than logged, so that an unreachable registry
    fails the run at the point of failure instead of emitting an empty dataset.

    Args:
        context: The context object for the current dataset.
    Returns:
        The session instance as a str.
    """
    resp = context.fetch_text(context.data_url)
    assert resp is not None, context.data_url
    matches = re.search(r"f\?p=18\d\:\d+\:(\d+)", resp)
    assert matches is not None, f"Cannot find secret param at {context.data_url}"
    return matches.group(1)


def renew_session(context: Context, state: CrawlState, reason: str) -> None:
    """Replace a broken APEX session with a fresh one."""
    state.renewals += 1
    # A healthy run needs a handful of renewals at most; past that the registry
    # is failing faster than we can recover.
    assert state.renewals <= 50, f"Too many session renewals, last: {reason}"
    context.log.info("Renewing registry session", reason=reason)
    state.instance = open_session(context)
    # The search returns no rows in a session that has not loaded the city list.
    seed_city(context, state)


def fetch_page(
    context: Context, url: str, data: dict[str, str] | None = None
) -> HtmlElement:
    """Fetch a registry page, which is UTF-8 even where the response omits it."""
    method = "GET" if data is None else "POST"
    return context.fetch_html(url, method=method, data=data, encoding="utf-8")


def clean_name(raw_name: str | None) -> list[str]:
    """
    Clean a single company name string, returning one name and any aliases found.

    If the input is None or empty, returns [None].
    """
    if not raw_name:
        return []
    cleaned = REMOVE_REGEX.sub("", raw_name).strip(" -:()")
    names = h.multi_split(cleaned, SPLITS)
    return names


def parse_records(doc: HtmlElement) -> list[Record]:
    """Read every label/value block on a registry page into a record.

    Values keep their raw text, because entity IDs are derived from them.
    """
    records: list[Record] = []
    for table in h.xpath_elements(doc, '//table[contains(@class, "vertical2")]'):
        record: Record = {}
        for row in h.xpath_elements(table, './tr[td[@class="L"]]'):
            label, value = h.xpath_elements(row, "./td", expect_exactly=2)
            record[h.element_text(label)] = h.element_text(value, squash=False)
        if record:
            records.append(record)
    return records


def seed_city(context: Context, state: CrawlState) -> list[dict[str, str]]:
    """
    Fetches the list of cities from the website.
    Args:
        context: The context object for the current dataset.
        state: The crawl state holding the registry session.
    Returns:
        The list of cities as a list of dicts.
    """
    payload = {
        "p_request": "APPLICATION_PROCESS=populateShuttleOps",
        "p_instance": state.instance,
        "p_flow_id": "186",
        "p_flow_step_id": "0",
        "x01": "",
        "x02": "",
        "x03": "-1",
        "x04": "-1",
    }
    resp = context.fetch_text(url=PROCESS_URL, method="POST", data=payload)
    assert resp is not None
    cities = re.findall(r'id: (\d+), data: "([\w /-]+)"', resp)

    return [{"city": city, "code": code} for code, city in cities]


def list_companies(
    context: Context, state: CrawlState, search: Search
) -> list[dict[str, str | None]]:
    """Run a search and return the companies it lists, at most `LISTING_CAP`."""
    params = {
        "p_request": "APPLICATION_PROCESS=NAPREDNA_PRETRAGA_PARAMS",
        "p_instance": state.instance,
        "p_flow_id": "186",
        "p_flow_step_id": "0",
        "x01": search.city,
        "x02": search.legal_form,
        "x03": "-1",
        "x04": "",
        "x05": search.registered_from,
        "x06": search.registered_to,
        "x07": "-1",
        "x08": "-1",
        "x09": "",
    }
    context.fetch_text(url=PROCESS_URL, method="POST", data=params)
    status_params = {
        "p_request": "APPLICATION_PROCESS=NAPREDNA_PRETRAGA_PARAMS_2",
        "p_instance": state.instance,
        "p_flow_id": "186",
        "p_flow_step_id": "0",
        "x01": search.bankrupt,
        "x02": search.removed,
        "x03": search.foreign_trade,
    }
    context.fetch_text(url=PROCESS_URL, method="POST", data=status_params)
    retrieve = {
        "p": f"186:3:{state.instance}:FLOW_PPR_OUTPUT_R16339113485096783_pg_"
        + "R_16339113485096783:NO",
        "pg_max_rows": "5000",
        "pg_min_row": "1",
        "pg_rows_fetched": "undefined",
    }
    result = fetch_page(context, PAGE_URL, data=retrieve)

    records = []
    for row in result.findall(".//tr"):
        if row.find(".//td/a") is None:
            continue
        record: dict[str, str | None] = {}
        record["registration_number"] = row[0].text
        record["name"] = row[1][0].text
        record["abbreviation"] = row[2][0].text
        record["address"] = row[3].text
        record["date_of_last_decision"] = row[4].text
        detail_hrefs = h.xpath_strings(row, ".//td/a/@href")
        record["details_url"] = (
            apex_url(urljoin(BASE_URL, detail_hrefs[0])) if detail_hrefs else None
        )
        records.append(record)
    return records


def list_checked(
    context: Context, state: CrawlState, search: Search
) -> list[dict[str, str | None]]:
    """Run a search, renewing the session if an empty result came from a dead one.

    A dead session answers a search exactly like a city without companies, so
    an empty listing is only trusted while the canary search still lists some.
    """
    records = list_companies(context, state, search)
    if records:
        if state.canary is None:
            state.canary = search
        return records
    if state.canary is None or list_companies(context, state, state.canary):
        return records
    renew_session(context, state, "the canary search listed no companies")
    return list_checked(context, state, search)


def fetch_report(
    context: Context, state: CrawlState, page: int, regno: str
) -> list[Record]:
    """Fetch the founders or managers of the company the session last opened.

    Every page of a paginated report is followed. The page is checked against
    the company's registration number, and must either list records or say that
    there are none, so that a page served for another company or by a broken
    session is never read as that company's data.

    Raises:
        SessionLost: The page does not belong to the expected company.
    """
    url = f"{PAGE_URL}?p=186:{page}:{state.instance}::NO::"
    try:
        doc = fetch_page(context, url)
    except requests.exceptions.RequestException as exc:
        raise SessionLost(f"page {page}: {exc}")
    # The detail link passes the registration number comma-separated from the
    # other APEX items, so the registry stores and shows a number such as
    # "1-373-00, INT. 364" only up to its first comma.
    expected = comparable_regno(regno.partition(",")[0])
    shown = h.xpath_elements(doc, '//span[@class="display_only"]')
    if not shown or comparable_regno(h.element_text(shown[0])) != expected:
        raise SessionLost(f"page {page} does not show {expected!r}")
    records = parse_records(doc)
    if not records and not h.xpath_elements(doc, '//span[@class="nodatafound"]'):
        raise SessionLost(f"page {page} has neither records nor a no-data note")

    for first_link in h.xpath_strings(doc, NEXT_PAGE_XPATH):
        link: str | None = first_link
        while link is not None:
            match = REGEX_REPORT_PAGE.search(link)
            assert match is not None, link
            region, min_row, max_rows, fetched = match.groups()
            data = {
                "p": f"186:{page}:{state.instance}:FLOW_PPR_OUTPUT_R{region}_pg_R_{region}:NO",
                "pg_max_rows": max_rows,
                "pg_min_row": min_row,
                "pg_rows_fetched": fetched,
            }
            try:
                fragment = fetch_page(context, PAGE_URL, data=data)
            except requests.exceptions.RequestException as exc:
                raise SessionLost(f"page {page} from row {min_row}: {exc}")
            more = parse_records(fragment)
            if not more:
                raise SessionLost(f"page {page} from row {min_row} is empty")
            records.extend(more)
            links = h.xpath_strings(fragment, NEXT_PAGE_XPATH)
            link = links[0] if links else None
    return records


def fetch_company(
    context: Context, state: CrawlState, record: dict[str, str | None]
) -> dict[str, Any] | None:
    """Fetch the detail, founders and managers pages of one company.

    Returns None when the registry's firewall rejects the detail link.

    Raises:
        SessionLost: A page does not belong to the expected company.
    """
    details_url = record["details_url"]
    regno = record["registration_number"]
    assert details_url is not None and regno is not None, record
    try:
        doc = fetch_page(context, details_url)
    except requests.exceptions.RequestException as exc:
        raise SessionLost(f"details: {exc}")
    # Some detail pages repeat the company's record block.
    details: list[Record] = []
    for detail in parse_records(doc):
        if detail not in details:
            details.append(detail)
    shown = (
        comparable_regno(details[0].get("Registration Number", "")) if details else None
    )
    if len(details) != 1 or shown != comparable_regno(regno):
        if WAF_REJECTION in h.element_text(doc):
            context.log.info("Details page rejected by firewall", url=details_url)
            state.errors += 1
            return None
        raise SessionLost(f"details page does not show {regno!r}")
    return {
        "details": details[0],
        "founders": fetch_report(context, state, FOUNDERS_PAGE, regno),
        "managers": fetch_report(context, state, MANAGERS_PAGE, regno),
    }


def emit_company(
    context: Context,
    record: dict[str, str | None],
    pages: dict[str, Any] | None,
    removed: bool,
) -> None:
    """Emit a company from its listing row, and from its pages where available.

    Args:
        removed: The company was struck from the court register.
    """
    entity = context.make("Company")
    reg_number = record.get("registration_number")
    if reg_number is not None and roughly_valid_regno(reg_number):
        entity.id = context.make_slug(reg_number)
    else:
        assert record["name"] is not None
        entity.id = context.make_id("BACompany", record["name"])

    # Abbreviation isn't so much an alias as simply a shortened but totally valid form
    # name:   MIDAX d.o.o. za proizvodnju, promet i usluge Banovići
    # google translate:
    #         MIDAX d.o.o. for production, trade and services Banovići
    # abbrev: MIDAX d.o.o. Banovići
    for raw_name in [record["name"], record["abbreviation"]]:
        if names := clean_name(raw_name):
            entity.add("name", names[0], lang="bos")
            entity.add("alias", names[1:], lang="bos")
    if not entity.has("name"):
        # The registry names some entries only ".", leaving nothing to emit.
        context.log.info("Skipping company without a name", url=record["details_url"])
        return

    entity.add("country", "ba")
    address: str | None = record["address"]
    if address != "-":
        entity.add("address", address, lang="bos")
    reg_number = (reg_number or "").strip()
    if REGEX_MBS.match(reg_number):
        entity.add("mbsCode", reg_number)
    elif re.search(r"\d", reg_number):
        entity.add("registrationNumber", reg_number)
    entity.add("modifiedAt", record["date_of_last_decision"])
    if removed:
        entity.add("status", "Brisan iz sudskog registra", lang="bos")

    if pages is None:
        context.emit(entity)
        return

    details: Record = dict(pages["details"])
    entity.add("legalForm", details.pop("Legal form of organization"), lang="bos")
    entity.add("address", details.pop("Address"), lang="bos")
    entity.add("status", details.pop("Status (Bankruptcy – YES/NO)"), lang="bos")
    jib = details.pop("Unique Identification Number").strip()
    if REGEX_JIB.match(jib):
        entity.add("jibCode", jib)
    elif re.search(r"\d", jib):
        entity.add("registrationNumber", jib)
    # The customs number is the PDV (VAT) number: the JIB without its leading 4.
    customs_number = details.pop("Customs Number").replace("\xa0", " ").strip()
    if customs_number.lower() not in ("", "-", "nema"):
        entity.add("vatCode", customs_number)
    context.audit_data(details, ignore=["Registration Number", "Name", "Abbreviation"])
    context.emit(entity)
    company_id = entity.id
    assert company_id is not None

    for founder_record in pages["founders"]:
        founder_record = dict(founder_record)
        if "Ime osnivača" in founder_record:
            emit_founder_person(context, company_id, founder_record)
        else:
            emit_founder_company(context, company_id, founder_record)
        context.audit_data(founder_record)

    for manager_record in pages["managers"]:
        manager_record = dict(manager_record)
        emit_manager(context, company_id, manager_record)
        context.audit_data(manager_record)


def emit_founder_person(context: Context, company_id: str, record: Record) -> None:
    name = record.pop("Ime osnivača")
    # The contracted capital is mostly equal to the paid-in capital, and there is
    # no property on Ownership to tell the two apart.
    record.pop("Kapital [ugovoreni]")
    capital_paid = record.pop("Kapital [uplaćeni]")
    shares = record.pop("Dionice [broj]")
    if name in FOUNDER_DENY_LIST:
        return
    founder = context.make("Person")
    founder.id = context.make_id("BAFounder", company_id, name)
    founder.add("name", name, lang="bos")
    if not founder.has("name"):
        context.log.info(
            "Skipping founder without a usable name", name=name, company=company_id
        )
        return
    context.emit(founder)

    own = context.make("Ownership")
    own.id = context.make_id("BAOwnership", company_id, founder.id)
    own.add("asset", company_id)
    own.add("owner", founder.id)
    own.add("role", "Founder", lang="eng")
    own.add("sharesValue", capital_paid)
    if shares.replace("\xa0", " ").strip("	 -"):
        own.add("sharesCount", shares)
    context.emit(own)


def split_registration_numbers(value: str) -> list[str]:
    """Split a founder's registration number field into the numbers it holds.

    Placeholders such as "-" or "-//-" yield nothing.
    """
    parts = [value]
    if match := REGEX_OLD_REGNO.match(value):
        parts = list(match.groups())
    numbers: list[str] = []
    for part in parts:
        numbers.extend(REGEX_REGNO_LABEL.split(part))
    return [n.strip() for n in numbers if re.search(r"\d", n)]


def emit_founder_company(context: Context, company_id: str, record: Record) -> None:
    basic_data = record.pop("Basic data")
    reg_num = record.pop("Registration Number")
    # The registry joins name, location and country with " ,", as in
    # 'OPĆINA FOJNICA ,Fojnica,  Bosanska bb, Fojnica ,Bosna I Hercegovina'. The
    # name itself occasionally contains " ,", so the country is the last part
    # and the location everything in between.
    parts = [p.strip() for p in basic_data.split(" ,")]
    name = parts[0]
    if name in FOUNDER_DENY_LIST:
        return
    if SHAREHOLDER_NOTE in name.lower():
        context.log.info(
            "Skipping shareholder note used as a founder name",
            name=name,
            company=company_id,
        )
        return
    if len(parts) < 3:
        context.log.warning(
            "Founder data is not name, location and country",
            basic_data=basic_data,
            company=company_id,
        )
        parts = [name, "", ""]

    founder = context.make("LegalEntity")
    founder.id = context.make_id("BAFounderCompany", company_id, name)
    founder.add("name", name, lang="bos")
    if not founder.has("name"):
        context.log.info(
            "Skipping founder company without a usable name",
            name=name,
            company=company_id,
        )
        return
    if parts[-1]:
        founder.add("country", parts[-1], lang="bos")
    location = [p.strip() for p in ",".join(parts[1:-1]).split(",")]
    address = ", ".join(p for p in location if p not in ("", "-"))
    if address:
        founder.add("address", address, lang="bos")
    founder.add("registrationNumber", split_registration_numbers(reg_num))
    context.emit(founder)

    own = context.make("Ownership")
    own.id = context.make_id("BAOwnership", company_id, founder.id)
    own.add("asset", company_id)
    own.add("owner", founder.id)
    own.add("role", "Founder", lang="eng")
    context.emit(own)


def emit_manager(context: Context, company_id: str, record: Record) -> None:
    name = record.pop("Name")
    position = record.pop("Position")
    authorisations = record.pop("Authorisations/ Position limits")
    director = context.make("Person")
    director.id = context.make_id("BAdirector", company_id, name)
    director.add("name", name, lang="bos")
    if not director.has("name"):
        context.log.info(
            "Skipping director without a usable name", name=name, company=company_id
        )
        return
    context.emit(director)

    rel = context.make("Directorship")
    rel.id = context.make_id("BADirectorship", company_id, director.id)
    rel.add("role", position, lang="bos")
    rel.add("description", authorisations, lang="bos")
    rel.add("director", director)
    rel.add("organization", company_id)
    context.emit(rel)


def crawl_company(
    context: Context,
    state: CrawlState,
    search: Search,
    record: dict[str, str | None],
) -> None:
    """Emit one company, from the record cache or freshly fetched pages.

    A company whose founders page lists nobody is held back in `state.deferred`
    instead, see `recheck_deferred`.

    Raises:
        SessionLost: A page does not belong to the expected company.
    """
    removed = search.removed == "1"
    details_url = record["details_url"]
    assert details_url is not None, record
    if apex_link_broken(details_url):
        context.log.info(
            "Details page unreachable: registry link has an unescaped colon",
            url=details_url,
        )
        emit_company(context, record, None, removed)
        return

    # Keyed on the date of the last registry decision, so that a company that
    # changed since it was cached is fetched again.
    cache_key = f"record:{company_key(details_url)}:{record['date_of_last_decision']}"
    cache_days: int = context.dataset.config["record_cache_days"]
    pages = context.cache.get_json(cache_key, max_age=cache_days)
    # The registry at times serves founders pages that list nobody for
    # companies that have founders, so such pages are neither cached nor
    # trusted from the cache.
    if pages is not None and not pages["founders"]:
        pages = None
    if pages is None:
        pages = fetch_company(context, state, record)
        if pages is None:
            emit_company(context, record, None, removed)
            return
        if not pages["founders"]:
            state.deferred[company_key(details_url)] = (search, record, pages)
            return
        context.cache.set_json(cache_key, pages)
    state.companies += 1
    emit_company(context, record, pages, removed)


def crawl_listed(
    context: Context,
    state: CrawlState,
    search: Search,
    records: list[dict[str, str | None]],
    only: set[str] | None = None,
) -> None:
    """Emit the companies a search listed, or only those with the given IDs.

    When a company's pages fail verification, the session is renewed and the
    search run again, because detail links only work in the session that listed
    them. A company that fails again on the fresh session is emitted from its
    listing row alone.
    """

    def selected(rows: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
        if only is None:
            return rows
        return [r for r in rows if company_key(str(r["details_url"])) in only]

    records = selected(records)
    done: set[str] = set()
    failed_once: set[str] = set()
    while True:
        current: dict[str, str | None] = {}
        try:
            for current in records:
                assert current["details_url"] is not None, current
                if company_key(current["details_url"]) in done:
                    continue
                crawl_company(context, state, search, current)
                done.add(company_key(current["details_url"]))
            return
        except SessionLost as exc:
            assert current["details_url"] is not None, current
            key = company_key(current["details_url"])
            if key not in failed_once:
                failed_once.add(key)
                renew_session(context, state, str(exc))
            else:
                context.log.warning(
                    "Company pages failed verification on a fresh session",
                    url=current["details_url"],
                    reason=str(exc),
                )
                state.errors += 1
                emit_company(context, current, None, search.removed == "1")
                done.add(key)
        listed = len(records)
        records = selected(list_checked(context, state, search))
        # A renewed session must list the same companies as the first one did.
        assert len(records) == listed, search


def recheck_deferred(context: Context, state: CrawlState) -> None:
    """Fetch the companies whose founders page listed nobody again, then emit all.

    The registry at times serves the founders report without its records for
    an hour or more, so the re-checks wait increasingly long. After the first
    round, they go on only while more companies lack founders than the run
    allows. Companies still without founders after the last round are emitted
    from the pages fetched for them.
    """
    delays: list[int] = context.dataset.config["recheck_delays_minutes"]
    max_share: float = context.dataset.config["max_share_without_founders"]
    for attempt, delay in enumerate(delays):
        pending = state.deferred
        share = len(pending) / max(1, state.companies + len(pending))
        if not pending or (attempt > 0 and share <= max_share):
            break
        context.log.info(
            "Re-checking companies without founders",
            companies=len(pending),
            share=f"{share:.1%}",
            delay_minutes=delay,
        )
        time.sleep(delay * 60)
        renew_session(context, state, "re-checking founders")
        state.deferred = {}
        by_search: dict[Search, set[str]] = {}
        for key, (search, _, _) in pending.items():
            by_search.setdefault(search, set()).add(key)
        for search, keys in by_search.items():
            records = list_checked(context, state, search)
            listed = {company_key(str(r["details_url"])) for r in records}
            for key in keys - listed:
                state.deferred[key] = pending[key]
            crawl_listed(context, state, search, records, only=keys)

    for search, record, pages in state.deferred.values():
        state.companies += 1
        state.without_founders += 1
        emit_company(context, record, pages, search.removed == "1")
    share = state.without_founders / max(1, state.companies)
    assert share <= max_share, f"Companies without founders: {share:.1%}"


def split_search(search: Search, periods: list[tuple[str, str]]) -> list[Search]:
    """Split a search that hit the listing cap into searches that cover it.

    Registration date periods come last, because a company without a
    registration date matches none of them.
    """
    if search.legal_form == "-1":
        return [replace(search, legal_form=form) for form in LEGAL_FORMS]
    if search.bankrupt == "-1":
        return [
            replace(search, bankrupt=bankrupt, foreign_trade=foreign_trade)
            for bankrupt in ("1", "0")
            for foreign_trade in ("1", "0")
        ]
    assert search.registered_from == "", f"Cannot split search further: {search}"
    return [
        replace(search, registered_from=from_date, registered_to=to_date)
        for from_date, to_date in periods
    ]


def crawl_search(
    context: Context,
    state: CrawlState,
    search: Search,
    periods: list[tuple[str, str]],
) -> int:
    """Emit the companies a search finds, splitting it while it hits the cap.

    Returns:
        The number of companies listed.
    """
    records = list_checked(context, state, search)
    if len(records) < LISTING_CAP:
        crawl_listed(context, state, search, records)
        return len(records)
    parts = split_search(search, periods)
    if parts[0].registered_from != "":
        context.log.info(
            "Splitting search by registration date, undated companies are missed",
            search=search,
        )
    total = sum(crawl_search(context, state, part, periods) for part in parts)
    # The parts must together list at least what the capped search listed.
    assert total >= LISTING_CAP, (search, total)
    return total


def generate_periods(
    from_date: datetime.date, to_date: datetime.date, step_months: int = 6
) -> list[tuple[str, str]]:
    """
    Generate periods for the given range of dates
    Args:
        from_date: The start date.
        to_date: The end date.
        step_months: The step in months.
    Returns:
        The list of periods as a list of tuples.
    """

    periods = []
    current_date = from_date
    while current_date < to_date:
        next_date = current_date + datetime.timedelta(days=step_months * 30)
        if next_date > datetime.date.today():
            periods.append((current_date.strftime("%d/%m/%Y"), ""))
        else:
            periods.append(
                (current_date.strftime("%d/%m/%Y"), next_date.strftime("%d/%m/%Y"))
            )
        current_date = next_date

    return periods


def crawl(context: Context) -> None:
    """
    Main function to crawl and process data from the Registers of business entities in
    Bosnia and Herzegovina
    """

    state = CrawlState(instance=open_session(context))
    cities = seed_city(context, state)

    periods: list[tuple[str, str]] = [
        ("01/01/1900", "01/01/2010"),
        # Some searches still list more than 500 companies registered within a
        # year, so the later periods are six months long.
    ] + generate_periods(
        from_date=datetime.date(2010, 1, 1),
        to_date=datetime.date.today(),
        step_months=6,
    )

    for city in cities:
        # Every search is split by register status, which the company pages
        # do not show.
        for removed in ("1", "0"):
            search = Search(city=city["code"], removed=removed)
            count = crawl_search(context, state, search, periods)
            context.log.debug(f"{city['city']}, removed={removed}: {count}")
    recheck_deferred(context, state)
    assert state.errors < EXPECTED_ERRORS, f"Too many errors: {state.errors}"
