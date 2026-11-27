import csv
import re
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast
from urllib.parse import urljoin, urlparse

import requests
from lxml import html
from rigour.mime.types import CSV
from zavod.entity import Entity

from zavod import Context
from zavod import helpers as h

LOCAL_PATH = Path(__file__).parent
# China started to publish counter-sanctions in 2019.
TWO_DIGIT_SANCTION_YEAR_BASE = 2010
INDEX_CACHE_DAYS = 2
MFA_INDEX_URL = "https://www.mfa.gov.cn/web/wjb_673085/zfxxgk_674865/gknrlb/fzcqdcs/"
MOFCOM_INDEX_URL = "https://www.mofcom.gov.cn/zcfb/blgg/gg/{year}/index.html"
MOFCOM_SPOKESPERSON_API = (
    "https://www.mofcom.gov.cn/api-gateway/jpaas-publish-server/front/page/build/unit"
)
MOFCOM_SPOKESPERSON_PARAMS = {
    "parseType": "bulidstatic",
    "webId": "8f43c7ad3afc411fb56f281724b73708",
    "tplSetId": "52551ea0e2c14bca8c84792f7aa37ead",
    "pageType": "column",
    "tagId": "分页列表",
    "pageId": "625b494d0adc47c885f5e1a18ee42b94",
    # The API returns at most the latest ~100 notices whatever pageSize says.
    "paramJson": '{"pageNo":1,"pageSize":100}',
}
MFA_NOTICE_PATH = re.compile(r"/(\d{6})/(t\d+_\d+)\.shtml$")
MOFCOM_NOTICE_KEY = re.compile(r"商务部公告(\d{4})年第(\d+)号")
MOFCOM_UEL_KEY = re.compile(r"不可靠实体清单工作机制公告〔(\d{4})〕(\d+)号")
MOFCOM_ARTICLE_PATH = re.compile(r"/zcfb/blgg/gg/\d{4}[^/]*/art/")
MOFCOM_CANDIDATE_TERMS = (
    "出口管制管控名单",
    "关注名单",
    "不可靠实体清单",
    "反制措施",
    "采取反制",
    "实施反制",
)

# TAO designation hub: aggregates the two formal Taiwan-independence lists and
# spokesperson announcements. Served as GB2312/GB18030, not UTF-8.
TAO_HUB_URL = "https://www.gwytb.gov.cn/zccs/zccs_61195/cjtdwgfz/"
TAO_NOTICE_PATH = re.compile(r"/(\d{6})/(t\d+_\d+)\.htm$")

# MOFCOM Bureau of Industry Security index. The static page is a JS shell; the notice
# list is served by a JPaaS CMS API whose front-end fetches the whole list in one call
# (pageSize=99999) and paginates client-side, so a single request returns everything.
MOFCOM_AQYGZJ_API = "https://aqygzj.mofcom.gov.cn/api-gateway/jpaas-publish-server/front/page/build/unit"
MOFCOM_AQYGZJ_PARAMS = {
    "webId": "b28941ad4e064442856787562c9a4961",
    "pageId": "79d6d2c4e44d458180d37dd4f0996645",
    "tagId": "信息列表",
    "tplSetId": "DDBav9QvwJVbs9iznQVmO",
    "parseType": "bulidstatic",
    "pageType": "column",
    "paramJson": '{"pageNo":1,"pageSize":99999}',
}
MOFCOM_AQYGZJ_ARTICLE_PATH = re.compile(
    r"/flzc/gzjgfxwj/art/\d{4}/art_[0-9a-f]{32}\.html"
)


@dataclass(frozen=True)
class Candidate:
    """Carry an official notice through human-review detection."""

    authority: str
    logical_key: str
    title: str
    url: str


def parse_mfa_index(content: str, base_url: str = MFA_INDEX_URL) -> list[Candidate]:
    """Find notices on the dedicated MFA anti-sanctions index."""
    root = html.fromstring(content)
    candidates: dict[str, Candidate] = {}
    for anchor in h.xpath_elements(root, "//a[@href]"):
        title = h.element_text(anchor)
        href = cast(str, anchor.get("href"))
        url = urljoin(base_url, href)
        match = MFA_NOTICE_PATH.search(urlparse(url).path)
        if match is None or len(title) == 0:
            continue
        logical_key = f"MFA-{match.group(1)}-{match.group(2)}"
        candidates[url] = Candidate("MFA", logical_key, title, url)
    return list(candidates.values())


def is_mofcom_candidate(title: str) -> bool:
    return any(term in title for term in MOFCOM_CANDIDATE_TERMS)


def mofcom_logical_key(title: str, url: str) -> str:
    """Derive a stable notice key so the same announcement keys identically across
    MOFCOM indexes. Prefers the announcement or UEL number, falling back to the URL
    filename when neither is present (e.g. 商务部令 orders with Chinese-numeral years)."""
    match = MOFCOM_NOTICE_KEY.search(title)
    if match is not None:
        return f"MOFCOM-{match.group(1)}-{int(match.group(2))}"
    match = MOFCOM_UEL_KEY.search(title)
    if match is not None:
        return f"MOFCOM-UEL-{match.group(1)}-{int(match.group(2))}"
    return f"MOFCOM-URL-{urlparse(url).path.rsplit('/', 1)[-1]}"


def parse_mofcom_index(content: str, base_url: str) -> list[Candidate]:
    """Find likely designation events on a MOFCOM annual announcement index."""
    root = html.fromstring(content)
    candidates: dict[str, Candidate] = {}
    for anchor in h.xpath_elements(root, "//a[@href]"):
        title = h.element_text(anchor)
        if len(title) == 0 or not is_mofcom_candidate(title):
            continue
        href = cast(str, anchor.get("href"))
        url = urljoin(base_url, href)
        candidates[url] = Candidate(
            "MOFCOM", mofcom_logical_key(title, url), title, url
        )
    return list(candidates.values())


def count_mofcom_articles(content: str) -> int:
    """Count annual links to detect an index layout or URL change."""
    root = html.fromstring(content)
    return sum(
        MOFCOM_ARTICLE_PATH.search(urlparse(cast(str, anchor.get("href"))).path)
        is not None
        for anchor in h.xpath_elements(root, "//a[@href]")
    )


def parse_tao_hub(content: str, base_url: str = TAO_HUB_URL) -> list[Candidate]:
    """Find designation and list-change notices on the TAO hub page.

    The hub aggregates the two formal Taiwan-independence lists, spokesperson
    announcements, and future designation categories that appear as new subpaths.
    TAO notice titles are inconsistent, so candidates are keyed on the article-URL
    pattern rather than keywords; non-designation links (law texts, reporting columns)
    are muted through reviewed_urls once seen.
    """
    root = html.fromstring(content)
    candidates: dict[str, Candidate] = {}
    for anchor in h.xpath_elements(root, "//a[@href]"):
        title = h.element_text(anchor)
        href = cast(str, anchor.get("href"))
        url = urljoin(base_url, href)
        match = TAO_NOTICE_PATH.search(urlparse(url).path)
        if match is None or len(title) == 0:
            continue
        # The hub links each list page twice: once under its name, once as a "更多>>"
        # ("more") navigation link. Keep the most informative title for review context.
        existing = candidates.get(url)
        if existing is not None and len(existing.title) >= len(title):
            continue
        logical_key = f"TAO-{match.group(1)}-{match.group(2)}"
        candidates[url] = Candidate("TAO", logical_key, title, url)
    return list(candidates.values())


def parse_mofcom_aqygzj(payload: Any, base_url: str) -> list[Candidate]:
    """Find designation notices on the MOFCOM industry-security JPaaS index.

    Adds coverage the annual blgg/gg indexes miss — 商务部令 countermeasure orders and
    some UEL Working Mechanism announcements. The rendered list markup is nested in the
    API response's `data.html` field; a shape change raises rather than yielding zero.
    """
    if not isinstance(payload, dict) or "data" not in payload:
        raise ValueError("MOFCOM industry-security response missing 'data'")
    inner = payload["data"]
    if not isinstance(inner, dict) or "html" not in inner:
        raise ValueError("MOFCOM industry-security response missing 'data.html'")
    root = html.fromstring(inner["html"])
    candidates: dict[str, Candidate] = {}
    for anchor in h.xpath_elements(root, "//a[@href]"):
        title = h.element_text(anchor)
        if len(title) == 0 or not is_mofcom_candidate(title):
            continue
        href = cast(str, anchor.get("href"))
        url = urljoin(base_url, href)
        if MOFCOM_AQYGZJ_ARTICLE_PATH.search(urlparse(url).path) is None:
            continue
        candidates[url] = Candidate(
            "MOFCOM", mofcom_logical_key(title, url), title, url
        )
    return list(candidates.values())


def parse_mofcom_spokesperson(payload: Any, base_url: str) -> list[Candidate]:
    """Find list-related spokesperson notices, including status changes."""
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        raise TypeError("MOFCOM spokesperson response missing 'data'")
    markup = payload["data"].get("html")
    if not isinstance(markup, str):
        raise TypeError("MOFCOM spokesperson response missing 'data.html'")
    root = html.fromstring(markup)
    candidates: dict[str, Candidate] = {}
    for anchor in h.xpath_elements(root, "//a[@href]"):
        title = h.element_text(anchor)
        href = cast(str, anchor.get("href"))
        url = urljoin(base_url, href)
        if not title or not is_mofcom_candidate(title):
            continue
        if "/xwfb/xwfyrth/" not in urlparse(url).path:
            continue
        candidates[url] = Candidate(
            "MOFCOM", mofcom_logical_key(title, url), title, url
        )
    return list(candidates.values())


def discover_candidates(context: Context) -> list[Candidate]:
    """Find official notices that may require a data update or review."""
    candidates: list[Candidate] = []
    try:
        content = context.fetch_text(
            MFA_INDEX_URL, cache_days=INDEX_CACHE_DAYS, encoding="utf-8"
        )
        mfa_candidates = parse_mfa_index(content or "")
        if len(mfa_candidates) < 10:
            context.log.warning(
                "MFA designation index yielded too few notice links",
                count=len(mfa_candidates),
                url=MFA_INDEX_URL,
            )
        candidates.extend(mfa_candidates)
    except requests.RequestException as exc:
        context.log.warning(
            "MFA designation index request failed",
            url=MFA_INDEX_URL,
            error=str(exc),
        )

    current_year = date.today().year
    for year in (current_year, current_year - 1):
        url = MOFCOM_INDEX_URL.format(year=year)
        try:
            content = (
                context.fetch_text(url, cache_days=INDEX_CACHE_DAYS, encoding="utf-8")
                or ""
            )
            article_count = count_mofcom_articles(content)
            if article_count == 0:
                context.log.warning(
                    "MOFCOM designation index yielded no announcement links",
                    year=year,
                    url=url,
                )
            candidates.extend(parse_mofcom_index(content, url))
        except requests.RequestException as exc:
            context.log.warning(
                "MOFCOM designation index request failed",
                year=year,
                url=url,
                error=str(exc),
            )

    try:
        payload = context.fetch_json(
            MOFCOM_SPOKESPERSON_API,
            params=MOFCOM_SPOKESPERSON_PARAMS,
            cache_days=INDEX_CACHE_DAYS,
        )
        candidates.extend(parse_mofcom_spokesperson(payload, MOFCOM_SPOKESPERSON_API))
    except requests.RequestException as exc:
        context.log.warning(
            "MOFCOM spokesperson index request failed",
            url=MOFCOM_SPOKESPERSON_API,
            error=str(exc),
        )

    try:
        payload = context.fetch_json(
            MOFCOM_AQYGZJ_API, params=MOFCOM_AQYGZJ_PARAMS, cache_days=INDEX_CACHE_DAYS
        )
        aqygzj_candidates = parse_mofcom_aqygzj(payload, MOFCOM_AQYGZJ_API)
        if len(aqygzj_candidates) == 0:
            context.log.warning(
                "MOFCOM industry-security index yielded no candidates",
                url=MOFCOM_AQYGZJ_API,
            )
        candidates.extend(aqygzj_candidates)
    except requests.RequestException as exc:
        context.log.warning(
            "MOFCOM industry-security index request failed",
            url=MOFCOM_AQYGZJ_API,
            error=str(exc),
        )

    try:
        content = context.fetch_text(
            TAO_HUB_URL, cache_days=INDEX_CACHE_DAYS, encoding="gb18030"
        )
        tao_candidates = parse_tao_hub(content or "")
        if len(tao_candidates) < 3:
            context.log.warning(
                "TAO designation hub yielded too few notice links",
                count=len(tao_candidates),
                url=TAO_HUB_URL,
            )
        candidates.extend(tao_candidates)
    except requests.RequestException as exc:
        context.log.warning(
            "TAO designation hub request failed",
            url=TAO_HUB_URL,
            error=str(exc),
        )

    return sorted(
        candidates, key=lambda item: (item.authority, item.logical_key, item.url)
    )


def collect_reviewed_urls(
    rows: list[dict[str, str]], discovery_config: Mapping[str, Any]
) -> set[str]:
    """Combine imported source URLs with notices resolved without data changes."""
    reviewed_urls = {
        str(url) for url in discovery_config.get("reviewed_urls", []) if url
    }
    for row in rows:
        reviewed_urls.add(row["Source URL"])
        if row.get("Current status source URL"):
            reviewed_urls.add(row["Current status source URL"])
    return reviewed_urls


def apply_current_status(
    context: Context,
    sanction: Entity,
    name: str,
    end_date: str | None,
    row: dict[str, str],
) -> tuple[str, bool] | None:
    """Add the latest official status of a designation row to its Sanction.

    The status phrase is emitted verbatim; the ``current_status`` lookup only decides
    whether the notice ends the row's measure or suspends it.

    Returns:
        The status date and whether it ends the measure, or None without a status.
    """
    phrase = row.pop("Current status (source)", None)
    status_date = row.pop("Current status date", None)
    row.pop("Current status notice title", None)
    status_url = row.pop("Current status source URL", None)
    quote = row.pop("Current status quote", None)
    language = row.pop("Current status quote language", None)
    if not phrase:
        if status_date or status_url or quote:
            raise ValueError(f"Status columns without a status phrase for {name!r}")
        return None
    if not status_date or not status_url or not quote or not language:
        raise ValueError(f"Incomplete current status columns for {name!r}")
    if phrase not in quote:
        raise ValueError(
            f"Status phrase {phrase!r} is not part of its quote ({name!r})"
        )
    res = context.lookup("current_status", phrase)
    if res is None:
        raise ValueError(f"Unmapped status phrase {phrase!r} for {name!r}")
    sanction.add("status", phrase, lang="zho")
    sanction.add("sourceUrl", status_url)
    sanction.add("provisions", quote, lang=language)
    if res.ends and end_date and end_date != status_date:
        raise ValueError(f"End date and status date differ for {name!r}")
    return status_date, bool(res.ends)


def parse_date(context: Context, value: str) -> str:
    """Parse a `DD.MM.YY(YY)` sheet date to ISO, or return an empty string."""
    dates = h.extract_date(
        context.dataset, value, two_digit_year_base=TWO_DIGIT_SANCTION_YEAR_BASE
    )
    return dates[0] if len(dates) > 0 else ""


def crawl_designation(
    context: Context, program_key: str | None, rows: list[dict[str, str]]
) -> None:
    """Emit a target and one Sanction spanning its rows under one program.

    China often designates the same target again under a program it is already
    listed on, and the MFA compiles its decisions onto an index page that is
    recorded as a second row. The Sanction is listed on the earliest notice and
    modified by every later notice, status change or end. Notices take effect on
    publication, so the earliest `Date` is also the `startDate`; a `Listing date`
    records a publication that precedes the effective date. A program can carry
    several measures against one target, so the Sanction ends only once every
    row has ended, and counts as suspended only while every row that has not
    ended is suspended.
    """
    rows.sort(key=lambda row: parse_date(context, row["Date"]))
    first = rows[0]
    entity = context.make(first["Type"])
    qid = first["QID"] or None
    entity.id = qid or context.make_id(first["Name"])
    entity.add("wikidataId", qid)
    program = first["List"] or None
    sanction = h.make_sanction(
        context,
        entity,
        key=program_key,
        program_name=program,
        program_key=program_key,
    )
    first_date = first["Date"]
    h.apply_date(sanction, "listingDate", first["Listing date"])
    h.apply_date(
        sanction,
        "startDate",
        first_date,
        two_digit_year_base=TWO_DIGIT_SANCTION_YEAR_BASE,
    )

    topics: set[str] = set()
    end_dates: list[str] = []
    suspended = 0
    for row in rows:
        if row.pop("Type") != entity.schema.name:
            raise ValueError(f"Rows of {entity.id} disagree on the schema")
        name = row.pop("Name")
        row.pop("QID")
        entity.add("name", name, lang="eng")
        entity.add("alias", row.pop("Alias").split(";"), lang="eng")
        entity.add("alias", row.pop("Chinese name"), lang="zho")
        entity.add("country", row.pop("Country", None))
        entity.add("address", row.pop("Address", None))
        entity.add("notes", row.pop("Summary", None), lang="eng")
        entity.add("notes", row.pop("Chinese summary", None), lang="zho")
        topics.update(row.pop("Topics").split(";"))
        sanction.add("program", row.pop("List", None))
        source_url = row.pop("Source URL", None)
        if not source_url:
            raise ValueError(f"Row for {name!r} has no Source URL")
        sanction.add("sourceUrl", source_url)
        sanction.add("authority", row.pop("Body", None))
        sanction.add("recordId", row.pop("Notice ID", None))
        row.pop("Notice title", None)
        designation_quote = row.pop("Designation quote", None)
        designation_language = row.pop("Designation quote language", None)
        if designation_quote:
            sanction.add("provisions", designation_quote, lang=designation_language)

        listing_date = row.pop("Listing date")
        designation_date = row.pop("Date")
        if designation_date != first_date:
            h.apply_date(
                sanction,
                "modifiedAt",
                listing_date or designation_date,
                two_digit_year_base=TWO_DIGIT_SANCTION_YEAR_BASE,
            )
        end_date = row.pop("End date", None)
        status = apply_current_status(context, sanction, name, end_date, row)
        if status is not None:
            status_date, ends = status
            h.apply_date(sanction, "modifiedAt", status_date)
            if ends:
                end_date = status_date
            elif not end_date:
                suspended += 1
        if end_date:
            h.apply_date(
                sanction,
                "modifiedAt",
                end_date,
                two_digit_year_base=TWO_DIGIT_SANCTION_YEAR_BASE,
            )
            end_dates.append(parse_date(context, end_date))
        context.audit_data(row)

    if len(end_dates) == len(rows):
        h.apply_date(sanction, "endDate", max(end_dates))
    h.finalize_sanction(
        context, sanction, effective_on_listing=True, amendments_dated=True
    )
    # An ended designation stays in the dataset as history but no longer
    # flags the target. A suspended one is announced to resume, so it warns.
    if h.is_active(sanction):
        if suspended + len(end_dates) == len(rows):
            entity.add("topics", "reg.warn")
        else:
            entity.add("topics", sorted(topics))
    context.emit(sanction)
    context.emit(entity)


def crawl(context: Context) -> None:
    source_file = LOCAL_PATH / "sanctions.csv"
    resource_path = context.get_resource_path("source.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, context.SOURCE_TITLE)

    with open(source_file, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    discovery_config = context.dataset.config.get("discovery", {})
    reviewed_urls = collect_reviewed_urls(rows, discovery_config)
    # One Sanction per target and program: a target listed under several MOFCOM
    # and MFA programs keeps one Sanction for each, as their status diverges.
    designations: dict[tuple[str, str | None], list[dict[str, str]]] = {}
    for row in rows:
        entity_id = row["QID"] or context.make_id(row["Name"])
        assert entity_id is not None, row["Name"]
        program_key = h.lookup_sanction_program_key(context, row["List"])
        designations.setdefault((entity_id, program_key), []).append(row)
    for (_, program_key), designation_rows in designations.items():
        crawl_designation(context, program_key, designation_rows)

    try:
        candidates = discover_candidates(context)
    except Exception as exc:
        context.log.warning(
            "Designation discovery failed",
            error=str(exc),
        )
        return

    for candidate in candidates:
        if candidate.url in reviewed_urls:
            continue
        context.log.warning(
            "Unreviewed designation notice",
            authority=candidate.authority,
            logical_key=candidate.logical_key,
            title=candidate.title,
            url=candidate.url,
        )
