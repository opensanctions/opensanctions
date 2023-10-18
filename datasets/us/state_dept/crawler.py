from typing import Optional
from xml.etree import ElementTree
from normality import collapse_spaces
from zavod import Context
from zavod import helpers as h


DATE_FORMAT = ["%B %d, %Y"]


def crawl_bio_page(context: Context, url: str):
    doc = context.fetch_html(url, cache_days=3)
    name = collapse_spaces(
        doc.xpath(".//h1[contains(@class, 'featured-content__headline')]")[
            0
        ].text_content()
    )
    title = None
    if name.startswith("Ambassador "):
        title = "Ambassador"
        name = name.replace("Ambassador ", "")
    if name.startswith("Dr. "):
        title = "Dr."
        name = name.replace("Dr. ", "")

    entity = context.make("Person")
    entity.id = context.make_slug(name)
    entity.add("name", name)
    entity.add("title", title)
    entity.add("sourceUrl", url)

    # FAQ: Only U.S. citizens may apply for an appointment to the career
    # Foreign Service. A candidate must be a U.S. citizen on the date an
    # application to the Foreign Service is submitted (for Generalists),
    # or upon applying to fill a vacancy announcement (for Specialists).
    # FAQ: As long as you are a U.S. citizen, you may apply for any Civil
    # Service position for which you qualify, even if you have other
    # nationalities.
    # https://careers.state.gov/faqs/
    entity.add("nationality", "us")

    description = doc.find(".//meta[@property='og:description']").get("content")
    description = description.replace(
        "[…]", "[...More on linked State Dept page]"
    )
    entity.add("description", description)

    position_container = doc.xpath(
        ".//p[contains(@class, 'article-meta__author-bureau')]"
    )[0]
    for br in position_container.xpath(".//br"):
        br.tail = " - " + br.tail if br.tail else " - "
    position_name = collapse_spaces(position_container.text_content())

    topics = ["gov.national"]
    if position_name == "The Secretary of State" or position_name.startswith(
        "Under Secretary"
    ):
        topics.append("gov.executive")

    position = h.make_position(context, position_name, country="us", topics=topics)

    dates = collapse_spaces(
        doc.xpath(".//p[contains(@class, 'article-meta__publish-date')]")[
            0
        ].text_content()
    )
    start_date, end_date = dates.split(" - ")
    if end_date == "Present":
        end_date = None
    start_date = h.parse_date(start_date, DATE_FORMAT)[0]
    end_date = h.parse_date(end_date, DATE_FORMAT)[0] if end_date else None

    occupancy = h.make_occupancy(
        context, entity, position, start_date=start_date, end_date=end_date
    )
    context.emit(entity, target=True)
    context.emit(position)
    context.emit(occupancy)


def crawl_index_page(context: Context, doc: ElementTree):
    for anchor in doc.xpath(".//a[contains(@class, 'biography-collection__link')]"):
        crawl_bio_page(context, anchor.get("href"))


def get_next_link(doc) -> Optional[str]:
    """because elements are falsy?!"""
    el = doc.find(".//a[@class='next page-numbers']")
    if el is not None:
        return el.get("href")


def crawl(context: Context) -> Optional[str]:
    doc = context.fetch_html(context.data_url, cache_days=3)
    crawl_index_page(context, doc)
    while next_link := get_next_link(doc):
        context.log.info(f"Crawling index page {next_link}")
        doc = context.fetch_html(next_link, cache_days=3)
        crawl_index_page(context, doc)
