from lxml.html import HtmlElement
from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.extract.zyte_api import fetch_html


def crawl_bio_page(context: Context, url: str) -> None:
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

    name = h.element_text(h.xpath_element(doc, name_xpath))
    assert name is not None
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
    # application to the Foreign Service is submitted (for Generalists),
    # or upon applying to fill a vacancy announcement (for Specialists).
    # FAQ: As long as you are a U.S. citizen, you may apply for any Civil
    # Service position for which you qualify, even if you have other
    # nationalities.
    # https://careers.state.gov/faqs/
    entity.add("citizenship", "us")

    meta_el = doc.find(".//meta[@property='og:description']")
    assert meta_el is not None
    description: str | None = meta_el.get("content")
    assert description is not None
    description = description.replace("[…]", "[...More on linked State Dept page]")
    entity.add("description", description)

    position_container = h.xpath_element(
        doc, ".//p[contains(@class, 'article-meta__author-bureau')]"
    )
    for br in h.xpath_elements(position_container, ".//br"):
        br.tail = " - " + br.tail if br.tail else " - "
    position_name = h.element_text(position_container)
    assert position_name is not None

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
    dates = h.element_text(
        h.xpath_element(doc, ".//p[contains(@class, 'article-meta__publish-date')]")
    )
    assert dates is not None
    start_date, end_date_raw = dates.split(" - ")
    end_date: str | None = None if end_date_raw == "Present" else end_date_raw

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )
    context.emit(entity)
    context.emit(position)
    if occupancy is not None:
        context.emit(occupancy)


def get_next_link(doc: HtmlElement) -> str | None:
    el = doc.find(".//a[@class='next page-numbers']")
    if el is not None:
        href: str | None = el.get("href")
        return href
    return None


def crawl_index_page(context: Context, url: str) -> str | None:
    bios_xpath = ".//a[contains(@class, 'biography-collection__link')]"
    context.log.info("Crawling index page", url=url)
    doc = fetch_html(
        context,
        url,
        bios_xpath,
        geolocation="US",
    )
    for anchor in h.xpath_elements(doc, bios_xpath):
        href: str | None = anchor.get("href")
        assert href is not None
        context.log.info("Crawling bio page", url=href)
        crawl_bio_page(context, href)
    return get_next_link(doc)


def crawl(context: Context) -> None:
    next_link: str | None = context.data_url
    while next_link:
        next_link = crawl_index_page(context, next_link)
