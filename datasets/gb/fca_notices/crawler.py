from urllib.parse import urlencode

from lxml.etree import _Element

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_html

# Funnelback uses 1-based start; pages step by 10.
PAGE_SIZE = 10

# Prefixes of the standard FCA disclaimer that ends the firm details.
DISCLAIMERS = (
    "Some firms may",
    "They may also",
    "Scammers may",
)
SECTION_XPATH = ".//section[@id='section-unauthorised-firm-details' or @id='section-clone-firm-details']"


def apply_prop(context: Context, entity: Entity, label: str, value: str) -> None:
    value = value.strip().strip(",")
    if not value:
        return
    prop = context.lookup_value("firm_details", label.lower())
    if prop is None:
        context.log.warning("Unhandled firm detail field", label=label, value=value)
        return
    else:
        entity.add(prop, h.multi_split(value, [","]))


def crawl_details_page(context: Context, entity: Entity, url: str) -> None:
    """Fetch the firm page, add its fields."""
    doc = fetch_html(
        context, url, ".//h1", html_source="httpResponseBody", cache_days=7
    )
    sections = h.xpath_elements(doc, SECTION_XPATH)
    if len(sections) == 0:
        return

    label: str | None = None
    for p in h.xpath_elements(sections[0], ".//p"):
        strong = h.xpath_elements(p, "./strong")
        value = ((strong[0].tail or "") if strong else h.element_text(p) or "").strip()
        if strong:
            label = (h.element_text(strong[0]) or "").rstrip(":").strip()
            apply_prop(context, entity, label, value)
        elif label is not None:
            if value.startswith(DISCLAIMERS):
                break
            apply_prop(context, entity, label, value)


def crawl_item(context: Context, item: _Element) -> None:
    """Parse one search-result <li>, build the entity, hand off to the detail page."""
    links = h.xpath_elements(item, ".//h3[@class='search-item__title']//a[@href]")
    if not links:
        return
    name = h.element_text(links[0])
    url = links[0].get("href")
    if not name or not url:
        return

    date_raw: str | None = None
    dates = h.xpath_elements(item, ".//p[contains(@class,'published-date')]")
    if dates:
        text = h.element_text(dates[0]) or ""
        date_raw = text.removeprefix("Published:").strip()

    entity = context.make("LegalEntity")
    entity.id = context.make_id(url)
    h.apply_reviewed_name_string(context, entity, string=name, llm_cleaning=True)
    entity.add("sourceUrl", url)
    entity.add("topics", "reg.warn")

    crawl_details_page(context, entity, url)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", date_raw)

    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    start = 1
    ITEMS_XPATH = ".//li[contains(@class,'search-item')]"
    while True:
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
            ITEMS_XPATH,
            html_source="httpResponseBody",
        )
        items = h.xpath_elements(doc, ITEMS_XPATH)

        if not items:
            break
        for item in items:
            crawl_item(context, item)

        if not h.xpath_elements(doc, ".//a[@title='Go to next page']"):
            break
        start += PAGE_SIZE
