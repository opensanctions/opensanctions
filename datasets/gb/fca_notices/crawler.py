import re
from itertools import count
from lxml.etree import _Element
from typing import Dict

from zavod import Context
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_html

BASE_URL = "https://www.fca.org.uk"
WARNING_LIST_URL = f"{BASE_URL}/consumers/warning-list-unauthorised-firms"
LIST_VALIDATOR = ".//table[@class='table table-striped cols-3']"

# Strip trailing status annotations FCA appends to names, e.g.:
#   "ATFX (clone of FCA authorised firm) (updated)"
#   "Pipvertex (new)"
_STATUS_RE = re.compile(
    r"\s*\((new|updated|clone of fca (?:authorised|registered|approved) (?:firm|fund))\)\s*",
    re.IGNORECASE,
)


def clean_name(raw: str) -> str:
    return _STATUS_RE.sub(" ", raw).strip()


def crawl_row(context: Context, row: Dict[str, _Element]) -> None:
    str_row = h.cells_to_str(row)
    raw_name = str_row.get("name")
    date_added = str_row.get("date_added")

    if not raw_name:
        context.log.warning("Empty name cell", row=str_row)
        return

    name = clean_name(raw_name)

    # The <a href="/news/warnings/<slug>"> inside the name cell gives a stable ID.
    link_els = h.xpath_elements(row["name"], ".//a[@href]")
    slug = link_els[0].get("href", "").rstrip("/").split("/")[-1] if link_els else None
    if not slug:
        context.log.warning("No href on name link; skipping", name=name)
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_slug(slug)
    h.apply_reviewed_name_string(context, entity, string=name, llm_cleaning=True)
    entity.add("country", "gb")
    entity.add("topics", "reg.action")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", date_added)

    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    last_page: int | None = None

    for page in count(0):
        params = f"items_per_page=100&page={page}"
        url = f"{WARNING_LIST_URL}?{params}"

        doc = fetch_html(
            context,
            url,
            unblock_validator=LIST_VALIDATOR,
            html_source="browserHtml",
            geolocation="GB",
            cache_days=1,
        )

        # On the first page, read the last-page number from the pager link.
        if page == 0:
            last_els = h.xpath_elements(doc, ".//a[@rel='last']")
            if last_els:
                href = last_els[0].get("href", "")
                for part in href.split("&"):
                    if part.startswith("page="):
                        last_page = int(part.split("=", 1)[1])
                        break

        table = h.xpath_element(doc, LIST_VALIDATOR)

        # Firm names use <th scope="row"> — retag to <td> for parse_html_table.
        for th in h.xpath_elements(table, ".//tbody//tr//th"):
            th.tag = "td"

        for row in h.parse_html_table(table):
            crawl_row(context, row)

        if last_page is not None and page >= last_page:
            break
