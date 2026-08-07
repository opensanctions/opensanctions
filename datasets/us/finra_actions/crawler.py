"""
# Occasional issues:

FINRA listing pages are newest-first and served through inconsistent caches.
Intermediate pages can temporarily render the "No results found" empty state
even though reruns later return rows; pagination can also drift if the listing
changes while a crawl is in progress. Stale Varnish entries can also return an
older slice of the (shifting) list, so records skip or duplicate across pages.

Mitigations:

- We sort by case ID (an unexposed but accepted sort key) instead of the
  default newest-first action date. Case IDs are near-unique, so the origin's
  unstable ordering among sort-key ties (which reshuffles rows across page
  boundaries between fetches under the date sort) has almost nothing to act
  on, and new cases get the highest IDs so they append at the end of the
  listing instead of shifting every page.
- Every request carries a `cache_bust` query parameter so Varnish treats each
  page URL as unique and fetches fresh from origin. The token varies per
  attempt, because Varnish caches whatever the origin returned for that exact
  URL — `cache_bust` included — with a one-year max-age, so retrying the
  identical URL is served the very response that just failed validation.
- The Zyte fetch validator requires a populated table and rejects the
  empty-state marker so those pages are retried and, if persistent, abort the
  crawl instead of emitting a partial run.
- The crawler aborts if the advertised last page changes after pagination has
  been established.
"""

from lxml.etree import _Element
from secrets import token_urlsafe
from time import sleep
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from zavod import Context, helpers as h
from zavod.extract import zyte_api

RESULT_ROW_VALIDATOR = (
    ".//table[not(ancestor-or-self::*//div"
    "[contains(concat(' ', normalize-space(@class), ' '), ' view-empty ')])]//tr[td]"
)
# Attempts per listing page, and the factor scaling the pause between them.
# These reproduce the defaults of zyte_api.fetch_html, whose own retries we
# switch off so that each attempt can carry a fresh cache-busting token.
PAGE_ATTEMPTS = 4
BACKOFF_FACTOR = 3


def crawl_item(context: Context, row: dict[str, _Element]) -> None:
    names = []
    name_els = row.pop("firms_individuals")
    for div_row in h.xpath_elements(name_els, ".//div[@class='row']"):
        # Select the text-bearing span directly, skipping the icon span which has no
        # text content. This avoids relying on positional span[2] and also prevents
        # the outer wrapper span from concatenating all descendant text into a single
        # corrupted string.
        name = h.xpath_string(div_row, "./span[normalize-space(text())]/text()").strip()
        if not name:
            context.log.warning("No name span found, page structure may have changed")
            continue
        names.extend(h.split_comma_names(context, name))
    case_summary = h.element_text(row.pop("case_summary"))
    case_id_el = row.pop("case_id")
    case_id = h.element_text(case_id_el)
    source_url = case_id_el.get("href")
    if source_url is not None:
        source_url = urljoin(context.data_url, source_url)
    date = h.element_text(row.pop("action_date"))

    for name in names:
        entity = context.make("LegalEntity")
        entity.id = context.make_slug(name)

        # Catches names with embedded alias indicators, e.g.:
        # "Score Priority Corp. formerly known as Just2Trade Inc."
        # "CODA Markets Inc. (f/k/a PDQ ATS Inc.)"
        h.apply_reviewed_name_string(
            context,
            entity,
            string=name,
            llm_cleaning=True,
        )

        entity.add("topics", "reg.action")
        entity.add("country", "us")
        context.emit(entity)

        sanction = h.make_sanction(context, entity, key=case_id)
        description = f"{date}: {case_summary}"
        sanction.add("description", description)
        sanction.add("authorityId", case_id)
        sanction.add("sourceUrl", source_url)
        h.apply_date(sanction, "date", date)
        context.emit(sanction)

    context.audit_data(row, ignore=["document_type"])


def get_max_page(response: _Element) -> int | None:
    links = h.xpath_elements(response, ".//a[contains(@title, 'Go to last page')]")
    if len(links) == 0:
        # Intermediate result pages incorrectly showing "No Results Found" have
        # no pagination links.
        return None
    assert len(links) == 1, len(links)
    href = links[0].get("href", "")
    params = parse_qs(urlparse(href).query)
    return int(params["page"][0])


def fetch_page(context: Context, page_num: int, cache_bust: str) -> _Element:
    """Fetch one listing page, giving every attempt its own cache-busting token.

    Varnish keys on the full URL and holds the response for a year, so a retry
    of the identical URL replays the empty-state page that just failed
    validation instead of asking the origin again.
    """
    for attempt in range(PAGE_ATTEMPTS):
        params = {
            "order": "field_fda_case_id_txt",
            "sort": "asc",
            "page": page_num,
            "cache_bust": f"{cache_bust}-{attempt}",
        }
        url = f"{context.data_url}?{urlencode(params)}"
        try:
            # Zyte because occasional cloudflare javascript challenge.
            return zyte_api.fetch_html(
                context,
                url,
                RESULT_ROW_VALIDATOR,
                absolute_links=True,
                retries=0,
            )
        except zyte_api.UnblockFailedException:
            if attempt + 1 == PAGE_ATTEMPTS:
                raise
            pause = BACKOFF_FACTOR * 2 ** (attempt + 1)
            context.log.info(
                f"Page {page_num} had no result rows, "
                f"sleeping {pause}s then retrying with a fresh token",
                page=page_num,
                attempt=attempt,
            )
            sleep(pause)
    raise AssertionError("Unreachable: the last attempt re-raises")


def crawl(context: Context) -> None:
    # Each page only displays 15 rows at a time. We determine the last page from
    # the pagination buttons because intermediate pages may report no results even
    # when later pages still have data.
    page_num = 0
    max_page = None
    # A single token per crawl bypasses Varnish's stale per-page entries; each
    # attempt at a page suffixes it so retries also reach the origin.
    cache_bust = token_urlsafe(8)
    prev_case_id = ""
    ordering_warned = False
    while max_page is None or page_num <= max_page:
        context.log.info(f"Crawling page {page_num} of {max_page}")
        response = fetch_page(context, page_num, cache_bust)

        # Check the page count each iteration in case pagination changes.
        new_max = get_max_page(response)
        if new_max is not None:
            if max_page is None:
                max_page = new_max
            elif new_max != max_page:
                raise RuntimeError(
                    "FINRA pagination changed during crawl: "
                    f"expected last page {max_page}, got {new_max} "
                    f"on page {page_num}"
                )

        table = response.find(".//table")
        assert table is not None, "Validated FINRA page did not contain a table"

        for row in h.parse_html_table(table):
            # Duplicate IDs are legitimate (one row per document of a case),
            # so equality is fine; only a decrease means the sort parameters
            # are no longer being respected.
            case_id = h.element_text(row["case_id"])
            if case_id < prev_case_id and not ordering_warned:
                context.log.warning(
                    "Case IDs are no longer in ascending order — is the "
                    "sort parameter still respected?",
                    case_id=case_id,
                    previous_case_id=prev_case_id,
                    page=page_num,
                )
                ordering_warned = True
            prev_case_id = case_id
            crawl_item(context, row)

        page_num += 1
