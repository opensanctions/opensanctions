from typing import Optional
from normality import collapse_spaces
from zavod import Context
from zavod import helpers as h
from zavod.util import ElementOrTree
import re


REGEX_ITEM = re.compile("(\d+\. )?(\(U\) )?(?P<name>[\w \.'’“”-]+),? ?\((?P<country>[\w /]+)\) (– )?(?P<reason>.+)$")


def crawl_section(context: Context, url: str, section: ElementOrTree):
    print("\n--------------")
    program = section.find(".//h3[@class='report__section-subtitle']").text_content()
    program = program.replace("(Generally Listed in Chronological Order)", "")
    program = collapse_spaces(program.replace("Since Previous Report", ""))
    print(program)
    print()

    items = section.findall(".//li")
    if len(items) == 0:
        items = section.findall(".//p")
    if len(items) == 0:
        context.log.warning(f"Empty list for program {program}")
    for item in items:
        item_text = collapse_spaces(item.text_content())
        if item_text == "":
            continue
        match = REGEX_ITEM.match(item_text)
        if match:
            name = collapse_spaces(match.group("name"))
            country = collapse_spaces(match.group("country"))
            reason = collapse_spaces(match.group("reason"))
            
            print(f"{name} | {country} | {reason}")
        else:
            context.log.warning(f"Cannot parse item", item=item_text)



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