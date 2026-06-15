import re
from lxml.etree import _Element
from urllib.parse import urlencode

from zavod import Context
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_html

# Funnelback uses 1-based start; pages step by 10 (num_ranks param has no effect)
PAGE_SIZE = 10

# Strip clone/status annotations the FCA appends to names, e.g.:
#   "Fortradefx (Clone of FCA authorised firm)"
#   "sbinvestmentfund.org/sbinvestmentfund.co (clone of FCA approved fund)"
_STATUS_RE = re.compile(
    r"\s*\((?:new|updated|clone of fca (?:authorised|registered|approved) (?:firm|fund))\)\s*",
    re.IGNORECASE,
)


def clean_name(raw: str) -> str:
    return _STATUS_RE.sub(" ", raw).strip()


def crawl_firm(context: Context, url: str, name: str, date_raw: str | None) -> None:
    doc = fetch_html(
        context, url, ".//h1", html_source="httpResponseBody", cache_days=7
    )

    entity = context.make("LegalEntity")
    entity.id = context.make_id(url)
    entity.add("name", name)
    entity.add("sourceUrl", url)
    entity.add("topics", "reg.action")

    section = h.xpath_elements(
        doc, ".//section[@id='section-unauthorised-firm-details']"
    )
    if len(section) == 0:
        context.log.warning("No details section found on firm page", url=url)
        context.emit(entity)
        return

    # Fields are <p><strong>Label:</strong> value</p>.
    # Multi-value fields (e.g. email) continue in subsequent <p> elements
    # without a <strong>; the FCA disclaimer paragraphs are identified by
    # not containing any contact-like content (no @, http, +digit).
    current_label: str | None = None
    raw_values: list[str] = []

    def flush_field(label: str, values: list[str]) -> None:
        combined = " ".join(
            v.strip().strip(",") for v in values if v.strip().strip(",")
        )
        for val in h.multi_split(combined, [","]):
            val = val.strip()
            if not val:
                continue
            lbl = label.lower()
            if lbl == "name":
                entity.add("name", val)
            elif lbl in ("website", "url", "web address"):
                entity.add("website", val)
            elif lbl in ("telephone", "tel", "mobile", "phone"):
                entity.add("phone", val)
            elif lbl in ("email", "e-mail"):
                entity.add("email", val)
            elif lbl == "address":
                addr = h.make_address(context, full=val, country="gb")
                h.copy_address(entity, addr)
            elif lbl in ("other information", "social media details"):
                entity.add("notes", f"{label}: {val}")
            else:
                context.log.warning(
                    "Unhandled firm detail field", label=label, value=val, url=url
                )

    for p in h.xpath_elements(section[0], ".//p"):
        strong_els = h.xpath_elements(p, "./strong")
        if strong_els:
            # New labelled field — flush the previous one first.
            if current_label is not None:
                flush_field(current_label, raw_values)
            label_text = h.element_text(strong_els[0]) or ""
            current_label = label_text.rstrip(":").strip()
            full_text = h.element_text(p) or ""
            # Value is the text after the <strong> label.
            raw_values = [full_text[len(label_text) :].strip()]
        elif current_label is not None:
            val = (h.element_text(p) or "").strip()
            # Stop accumulating when we hit the standard FCA disclaimer paragraph.
            if val.startswith("Some firms may") or val.startswith("They may also"):
                break
            if val:
                raw_values.append(val)

    if current_label is not None:
        flush_field(current_label, raw_values)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", date_raw)

    context.emit(entity)
    context.emit(sanction)


def crawl_page(context: Context, start: int) -> tuple[list[_Element], _Element]:
    """Fetch one search-results page; return (items, doc)."""
    params = urlencode(
        {
            "n_search_term": "",
            "category": "warnings",
            "sort_by": "dmetaZ",
            "start": start,
        }
    )
    url = f"{context.dataset.url}?{params}"
    doc = fetch_html(
        context,
        url,
        ".//li[contains(@class,'search-item')]",
        html_source="httpResponseBody",
        cache_days=1,
    )
    items = h.xpath_elements(doc, ".//li[contains(@class,'search-item')]")
    return items, doc


def crawl(context: Context) -> None:
    start = 1

    while True:
        items, doc = crawl_page(context, start)

        if not items:
            break

        for item in items:
            title_link = h.xpath_elements(
                item, ".//h3[@class='search-item__title']//a[@href]"
            )
            if not title_link:
                continue
            link = title_link[0]
            raw_name = h.element_text(link)
            href = link.get("href", "")
            if not raw_name or not href:
                continue
            name = clean_name(raw_name)
            firm_url = (
                href if href.startswith("http") else f"https://www.fca.org.uk{href}"
            )

            date_el = h.xpath_elements(item, ".//p[contains(@class,'published-date')]")
            date_raw: str | None = None
            if date_el:
                # "Published: 12/06/2026" → strip prefix
                date_raw = (
                    (h.element_text(date_el[0]) or "")
                    .removeprefix("Published:")
                    .strip()
                )

            crawl_firm(context, firm_url, name, date_raw)

        if len(h.xpath_elements(doc, ".//a[@title='Go to next page']")) == 0:
            break
        start += PAGE_SIZE
