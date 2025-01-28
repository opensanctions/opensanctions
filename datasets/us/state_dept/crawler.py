from typing import Optional
from normality import collapse_spaces
from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html


def crawl_bio_page(context: Context, url: str):
    name_xpath = ".//h1[contains(@class, 'featured-content__headline')]"
    doc = fetch_html(
        context,
        url,
        name_xpath,
        javascript=True,
        geolocation="US",
        cache_days=30,
    )
    if doc.xpath(".//body[contains(@class, 'error404')]"):
        if context.lookup("expected_404", url):
            return
        else:
            context.log.warning(
                "Unexpected 404. Perhaps the expected_404 list needs to be updated.",
                url=url,
            )

    name = collapse_spaces(doc.xpath(name_xpath)[0].text_content())
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
    description = description.replace("[…]", "[...More on linked State Dept page]")
    entity.add("description", description)

    position_container = doc.xpath(
        ".//p[contains(@class, 'article-meta__author-bureau')]"
    )[0]
    for br in position_container.xpath(".//br"):
        br.tail = " - " + br.tail if br.tail else " - "
    position_name = collapse_spaces(position_container.text_content())

    topics = ["gov.national"]
    if (
        position_name == "Secretary of State"
        or position_name.startswith("Deputy Secretary of State")
        or position_name.startswith("Under Secretary")
    ):
        topics.append("gov.executive")

    position = h.make_position(context, position_name, country="us", topics=topics)
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    dates = collapse_spaces(
        doc.xpath(".//p[contains(@class, 'article-meta__publish-date')]")[
            0
        ].text_content()
    )
    start_date, end_date = dates.split(" - ")
    if end_date == "Present":
        end_date = None

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )
    context.emit(entity, target=True)
    context.emit(position)
    context.emit(occupancy)


def get_next_link(doc) -> Optional[str]:
    el = doc.find(".//a[@class='next page-numbers']")
    if el is not None:
        return el.get("href")


def crawl_index_page(context: Context, url: str):
    bios_xpath = ".//a[contains(@class, 'biography-collection__link')]"
    context.log.info("Crawling index page", url=url)
    doc = fetch_html(
        context,
        url,
        bios_xpath,
        geolocation="US",
    )
    for anchor in doc.xpath(bios_xpath):
        context.log.info("Crawling bio page", url=anchor.get("href"))
        crawl_bio_page(context, anchor.get("href"))
    return get_next_link(doc)


def crawl(context: Context) -> Optional[str]:
    next_link = context.data_url
    while next_link:
        next_link = crawl_index_page(context, next_link)
