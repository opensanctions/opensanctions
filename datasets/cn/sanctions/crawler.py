import csv
import re
import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, cast
from urllib.parse import urljoin, urlparse

import requests
from lxml import html
from rigour.mime.types import CSV
from zavod import Context
from zavod import helpers as h


LOCAL_PATH = Path(__file__).parent
INDEX_CACHE_DAYS = 2
MFA_INDEX_URL = "https://www.mfa.gov.cn/web/wjb_673085/zfxxgk_674865/gknrlb/fzcqdcs/"
MOFCOM_INDEX_URL = "https://www.mofcom.gov.cn/zcfb/blgg/gg/{year}/index.html"
MFA_NOTICE_PATH = re.compile(r"/(\d{6})/(t\d+_\d+)\.shtml$")
MOFCOM_NOTICE_KEY = re.compile(r"商务部公告(\d{4})年第(\d+)号")
MOFCOM_UEL_KEY = re.compile(r"不可靠实体清单工作机制公告〔(\d{4})〕(\d+)号")
MOFCOM_ARTICLE_PATH = re.compile(r"/zcfb/blgg/gg/\d{4}[^/]*/art/")
MOFCOM_CANDIDATE_PATTERNS = (
    re.compile(r"列入.*(?:出口管制管控名单|关注名单|不可靠实体清单)"),
    re.compile(r"采取反制"),
    re.compile(
        r"(?:移出|暂停|恢复|继续暂停|停止|取消|调整).*"
        r"(?:出口管制管控名单|关注名单|不可靠实体清单|反制措施)"
    ),
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
    return any(pattern.search(title) for pattern in MOFCOM_CANDIDATE_PATTERNS)


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
    reviewed_urls.update(row["Source URL"] for row in rows if row["Source URL"])
    return reviewed_urls


def crawl(context: Context) -> None:
    source_file = LOCAL_PATH / "sanctions.csv"
    resource_path = context.get_resource_path("source.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, context.SOURCE_TITLE)

    with open(source_file, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    discovery_config = context.dataset.config.get("discovery", {})
    reviewed_urls = collect_reviewed_urls(rows, discovery_config)
    for row in rows:
        entity = context.make(row.pop("Type"))
        name = row.pop("Name")
        if name is None:
            continue
        qid = row.pop("QID", None)
        entity.id = qid or context.make_id(name)
        entity.add("wikidataId", qid)
        entity.add("name", name, lang="eng")
        entity.add("alias", row.pop("Alias").split(";"), lang="eng")
        entity.add("alias", row.pop("Chinese name"), lang="zho")
        entity.add("country", row.pop("Country", None))
        entity.add("address", row.pop("Address", None))
        entity.add("notes", row.pop("Summary", None), lang="eng")
        entity.add("notes", row.pop("Chinese summary", None), lang="zho")
        entity.add("topics", row.pop("Topics").split(";"))
        program = row.pop("List", None)
        sanction = h.make_sanction(
            context,
            entity,
            program_name=program,
            program_key=h.lookup_sanction_program_key(context, program),
        )
        sanction.set("authority", row.pop("Body", None))
        h.apply_date(sanction, "startDate", row.pop("Date", None))
        h.apply_date(sanction, "endDate", row.pop("End date", None))
        sanction.add("sourceUrl", row.pop("Source URL", None))
        context.emit(sanction)
        context.emit(entity)
        context.audit_data(row)

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
