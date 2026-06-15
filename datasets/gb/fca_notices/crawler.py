import re
from urllib.parse import urlencode

from lxml.etree import _Element

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_html

# Funnelback uses 1-based start; pages step by 10.
PAGE_SIZE = 10

# Strip clone/status annotations the FCA appends to names, e.g.
# "Fortradefx (Clone of FCA authorised firm)".
STATUS_RE = re.compile(
    r"\s*\((?:new|updated|clone of fca "
    r"(?:authorised|registered|approved) (?:firm|fund))\)\s*",
    re.IGNORECASE,
)

LABEL_PROPS = {
    "name": "name",
    "website": "website",
    "url": "website",
    "web address": "website",
    "telephone": "phone",
    "tel": "phone",
    "mobile": "phone",
    "phone": "phone",
    "email": "email",
    "e-mail": "email",
}


def clean_name(raw: str) -> str:
    return STATUS_RE.sub(" ", raw).strip()


def flush_field(
    context: Context, entity: Entity, label: str, values: list[str]
) -> None:
    combined = " ".join(v.strip().strip(",") for v in values if v.strip().strip(","))
    label = label.lower()
    for value in h.multi_split(combined, [","]):
        value = value.strip()
        if not value:
            continue
        prop = LABEL_PROPS.get(label)
        if prop is not None:
            entity.add(prop, value)
        elif label == "address":
            address = h.make_address(context, full=value)
            h.copy_address(entity, address)
        elif label in ("other information", "social media details"):
            entity.add("notes", f"{label}: {value}")
        else:
            context.log.warning("Unhandled firm detail field", label=label, value=value)


def crawl_firm(context: Context, url: str, name: str, date_raw: str | None) -> None:
    doc = fetch_html(context, url, ".//h1", html_source="httpResponseBody", cache_days=7)

    entity = context.make("LegalEntity")
    entity.id = context.make_id(url)
    entity.add("name", name)
    entity.add("sourceUrl", url)
    entity.add("topics", "reg.warn")

    sections = h.xpath_elements(
        doc, ".//section[@id='section-unauthorised-firm-details']"
    )
    if not sections:
        context.log.warning("No details section found on firm page", url=url)
        context.emit(entity)
        return

    # Fields are <p><strong>Label:</strong> value</p>; multi-value fields continue
    # in subsequent <p> elements until the FCA disclaimer paragraph.
    label: str | None = None
    values: list[str] = []
    for p in h.xpath_elements(sections[0], ".//p"):
        strong = h.xpath_elements(p, "./strong")
        if strong:
            if label is not None:
                flush_field(context, entity, label, values)
            label_text = h.element_text(strong[0]) or ""
            label = label_text.rstrip(":").strip()
            values = [(h.element_text(p) or "")[len(label_text):].strip()]
        elif label is not None:
            value = (h.element_text(p) or "").strip()
            if value.startswith("Some firms may") or value.startswith("They may also"):
                break
            if value:
                values.append(value)
    if label is not None:
        flush_field(context, entity, label, values)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", date_raw)
    context.emit(entity)
    context.emit(sanction)


def crawl_page(context: Context, start: int) -> tuple[list[_Element], _Element]:
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
            links = h.xpath_elements(
                item, ".//h3[@class='search-item__title']//a[@href]"
            )
            if not links:
                continue
            raw_name = h.element_text(links[0])
            href = links[0].get("href", "")
            if not raw_name or not href:
                continue
            firm_url = href if href.startswith("http") else f"https://www.fca.org.uk{href}"

            date_raw: str | None = None
            dates = h.xpath_elements(item, ".//p[contains(@class,'published-date')]")
            if dates:
                date_raw = (h.element_text(dates[0]) or "").removeprefix("Published:").strip()

            crawl_firm(context, firm_url, clean_name(raw_name), date_raw)

        if not h.xpath_elements(doc, ".//a[@title='Go to next page']"):
            break
        start += PAGE_SIZE
