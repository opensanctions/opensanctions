import re
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from lxml.etree import _Element

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_html

FCA_URL = "https://www.fca.org.uk/"
# The FCA's clone annotation, e.g. "(clone)" or "(Clone of FCA authorised firm)",
# but not a firm that merely has "clone" in its name.
CLONE_ANNOTATION = re.compile(r"\(clone\b[^)]*\)", re.IGNORECASE)

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
    elif prop == "address":
        entity.add(prop, value)
    elif prop == "name":
        # Repeats the warning list title, which is the name we use.
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
    if sections[0].get("id") == "section-clone-firm-details":
        entity.add("topics", "corp.clone")

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
    """Parse one warning list table row, build the entity, hand off to the detail page."""
    links = h.xpath_elements(item, "./th/a[@href]")
    if not links:
        return
    name = h.strip_name_titles(context, h.element_text(links[0]))
    href = links[0].get("href")
    if not name or not href:
        return
    url = urljoin(FCA_URL, href)
    if not url.startswith(FCA_URL):
        context.log.warning("Skipping off-site warning link", url=url)
        return

    date_raw: str | None = None
    dates = h.xpath_elements(item, "./td[contains(@class,'published-date')]")
    if dates:
        date_raw = h.element_text(dates[0])

    entity = context.make("LegalEntity")
    entity.id = context.make_id(url)
    h.apply_reviewed_name_string(context, entity, string=name, llm_cleaning=True)
    entity.add("sourceUrl", url)
    entity.add("topics", "reg.warn")
    # Older warning pages have no clone section, only the clone annotation in the name.
    if CLONE_ANNOTATION.search(name):
        entity.add("topics", "corp.clone")

    crawl_details_page(context, entity, url)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", date_raw)

    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    page = 0
    last_page: int | None = None
    ITEMS_XPATH = ".//div[contains(@class,'view-content')]//table/tbody/tr"
    while True:
        params = urlencode(
            {
                "items_per_page": 100,
                "order": "field_published_date",
                "sort": "desc",
                "page": page,
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
        if last_page is None:
            last_href = h.xpath_string(doc, ".//a[@title='Go to last page']/@href")
            last_page = int(parse_qs(urlparse(last_href).query)["page"][0])

        if not items:
            break
        for item in items:
            crawl_item(context, item)

        if not h.xpath_elements(doc, ".//a[@title='Go to next page']"):
            break
        page += 1

    if last_page is None or page < last_page:
        context.log.error(
            "Crawl ended before the last page", page=page, last_page=last_page
        )
