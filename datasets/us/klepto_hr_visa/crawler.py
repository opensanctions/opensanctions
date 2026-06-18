from normality import squash_spaces
from followthemoney.types import registry
import re
from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_html


REGEX_ITEM = re.compile(
    r"(\d+\. )?(\(U\) )?(?P<name>[\w \.'’“”-]+),? ?\((?P<country>[\w /]+)\) (– )?(?P<reason>.+)$"
)


def crawl_section(context: Context, url: str, section: HtmlElement) -> None:
    listing_titles = h.xpath_elements(
        section, "ancestor::section[contains(@class, 'report__content__inner')]/h2"
    )
    year: str | None = None
    for title in listing_titles:
        title_text = h.element_text(title)
        m = re.match(r"(2\d{3})", title_text)
        if m:
            year = m.group(1)

    program = h.element_text(section.find(".//h3[@class='report__section-subtitle']"))
    program = program.replace("(Generally Listed in Chronological Order)", "")
    program = squash_spaces(program.replace("Since Previous Report", ""))

    items = section.findall(".//li")
    if len(items) == 0:
        items = section.findall(".//p")
    if len(items) == 0:
        context.log.warning(f"Empty list for program {program}")

    for item in items:
        item_text = h.element_text(item)
        if item_text == "":
            continue
        if item_text.startswith("* Denotes an action"):
            continue

        match = REGEX_ITEM.match(item_text)
        if match:
            name = squash_spaces(match.group("name"))
            countries = [squash_spaces(c) for c in match.group("country").split("/")]
            reason = squash_spaces(match.group("reason"))

            if not all([registry.country.clean(c) for c in countries]):
                res = context.lookup("unparsed", item_text)
                if res is None:
                    context.log.warning("Cannot parse country", item=item_text)
                    continue
                name = res.name
                reason = res.reason
                countries = res.countries

            entity = context.make("Person")
            entity.id = context.make_id(item_text)
            entity.add("name", name)
            entity.add("notes", item_text)
            entity.add("country", countries)
            entity.add("topics", "sanction")

            sanction = h.make_sanction(context, entity)
            sanction.add("program", program)
            sanction.add("reason", reason)
            sanction.add("sourceUrl", url)
            sanction.add("listingDate", year)

            context.emit(entity)
            context.emit(sanction)
        else:
            context.log.warning("Cannot parse item", item=item_text)


def crawl_report(context: Context, url: str) -> None:
    context.log.info(f"Crawling {url}")
    sections_xpath = ".//section[@class='entry-content']"
    doc = fetch_html(context, url, sections_xpath, cache_days=1)
    for section in doc.findall(sections_xpath):
        crawl_section(context, url, section)


def crawl(context: Context) -> None:
    options_xpath = ".//nav[@id='report-nav']//option"
    doc = fetch_html(context, context.data_url, options_xpath, cache_days=1)
    options_el = h.xpath_elements(doc, options_xpath)
    for option in options_el:
        url = option.get("value")
        if url is not None and url != "":
            crawl_report(context, url)
