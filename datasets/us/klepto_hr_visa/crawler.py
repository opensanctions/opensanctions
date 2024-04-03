from typing import Optional
from normality import collapse_spaces
from followthemoney.types import registry
import re

from zavod import Context
from zavod import helpers as h
from zavod.util import ElementOrTree


REGEX_ITEM = re.compile(
    "(\d+\. )?(\(U\) )?(?P<name>[\w \.'’“”-]+),? ?\((?P<country>[\w /]+)\) (– )?(?P<reason>.+)$"
)


def crawl_section(context: Context, url: str, section: ElementOrTree):
    listing_titles = section.xpath(".//h3[contains(@class, 'report__section-title')]")
    if len(listing_titles) == 1:
        year = re.match("(\d{4})$", listing_titles[0]).group(1)
    else:
        year = None

    program = section.find(".//h3[@class='report__section-subtitle']").text_content()
    program = program.replace("(Generally Listed in Chronological Order)", "")
    program = collapse_spaces(program.replace("Since Previous Report", ""))

    items = section.findall(".//li")
    if len(items) == 0:
        items = section.findall(".//p")
    if len(items) == 0:
        context.log.warning(f"Empty list for program {program}")

    for item in items:
        item_text = collapse_spaces(item.text_content())
        if item_text == "":
            continue
        if item_text.startswith("* Denotes an action"):
            continue

        match = REGEX_ITEM.match(item_text)
        if match:
            name = collapse_spaces(match.group("name"))
            countries = [collapse_spaces(c) for c in match.group("country").split("/")]
            reason = collapse_spaces(match.group("reason"))

            if not all([registry.country.clean(c) for c in countries]):
                res = context.lookup("unparsed", item_text)
                name = res.name
                countries = res.country
                reason = res.reason

            entity = context.make("Person")
            entity.id = context.make_slug(countries, name)
            entity.add("name", name)
            entity.add("country", countries)
            entity.add("topics", "sanction")

            sanction = h.make_sanction(context, entity)
            sanction.add("program", program)
            sanction.add("reason", reason)
            sanction.add("sourceUrl", url)
            sanction.add("listingDate", year)

            context.emit(entity, target=True)
            context.emit(sanction)
        else:
            context.log.warning("Cannot parse item", item=item_text)


def crawl_report(context: Context, url: str):
    context.log.info(f"Crawling {url}")
    doc = context.fetch_html(url, cache_days=1)
    for section in doc.findall(".//section[@class='entry-content']"):
        crawl_section(context, url, section)


def crawl(context: Context) -> Optional[str]:
    doc = context.fetch_html(context.data_url, cache_days=1)
    for option in doc.find(".//nav[@id='report-nav']").xpath(".//option"):
        url = option.get("value")
        if url != "":
            crawl_report(context, url)
