from typing import Optional
from normality import collapse_spaces
from zavod import Context
from zavod import helpers as h
from zavod.util import ElementOrTree
import re


def crawl_section(context: Context, url: str, section: ElementOrTree):
    program = section.find(".//h3[@class='report__section-subtitle']").text_content()
    for item in section.findall(".//li"):
        name_el = item.find("./strong")
        if name_el is None:
            name_el = item.find("./b")
        name = name_el.text_content()
        country, reason = name_el.tail.split(")", 1)
        country = re.sub("[(–]", "", country)
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