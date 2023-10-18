from typing import Optional
from normality import collapse_spaces
from zavod import Context
from zavod import helpers as h
from zavod.util import ElementOrTree
import re


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
        name, rest = item.text_content().split("(", 1)
        country, reason = rest.split(")", 1)

        name = collapse_spaces(name)
        country = collapse_spaces(country)
        reason = collapse_spaces(re.sub("^\s*–\s*", "", reason))
        
        print(f"{name} | {country} | {reason}")



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