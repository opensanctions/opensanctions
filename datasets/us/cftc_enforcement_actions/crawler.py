from pprint import pprint
from typing import Optional, List, Literal
from pydantic import BaseModel, Field

from lxml.etree import tostring

from zavod.context import Context
from zavod import helpers as h
from zavod.shed.gpt import run_text_prompt, run_typed_text_prompt
from zavod.stateful.extraction import extract_items


class Associate(BaseModel):
    name: str
    address: Optional[str] = Field(
        description=(
            "The address or even just the state of the defendant, "
            "if in the source text."
        )
    )


class Defendant(BaseModel):
    schema: Literal["Person", "Company", "LegalEntity"]
    name: str
    aliases: Optional[List[str]] = None
    address: Optional[str] = Field(
        description=(
            "The address or even just the state of the defendant,"
            " if in the source text."
        )
    )
    country: Optional[str] = None
    owners: Optional[List[str]] = Field(
        description=(
            "The names of the owners of a Company defendant, " "if in the source text."
        )
    )
    associates: Optional[List[Associate]] = None
    authority: Optional[str] = None


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = """
Extract the defendants or entities added to the Red List in the attached article.
"""


def crawl_enforcement_action(context: Context, date: str, url: str) -> None:
    doc = context.fetch_html(url, cache_days=30)
    article = doc.xpath(".//article")[0]
    html = tostring(article, pretty_print=True).decode("utf-8")
    result = run_typed_text_prompt(context, PROMPT, html, response_type=Defendants)
    for item in extract_items(
        context,
        key=url,
        raw_data=result.defendants,
        source_url=url,
        model_type=Defendant,
    ):
        entity = context.make(item.schema)
        entity.add("name", item.name)
        entity.add("address", item.address)
        entity.add("country", item.country)
        entity.add("aliases", item.aliases)


def crawl_index_page(context: Context, doc) -> None:
    table_xpath = ".//div[contains(@class, 'view-content')]//table"
    tables = doc.xpath(table_xpath)
    assert len(tables) == 1
    for row in h.parse_html_table(tables[0]):
        date = row["date"]
        action_cell = row["enforcement_actions"]
        # Remove related links so we can assert that there's one key link
        for ul in action_cell.xpath(".//ul"):
            ul.getparent().remove(ul)
        urls = action_cell.xpath(".//a/@href")
        assert len(urls) == 1
        url = urls[0]
        crawl_enforcement_action(context, date, url)


def crawl(context: Context) -> None:
    next_url: Optional[str] = context.data_url
    while next_url:
        # Dev
        if "1" in next_url:
            break
        doc = context.fetch_html(next_url)
        doc.make_links_absolute(next_url)
        next_urls = doc.xpath(".//a[@rel='next']/@href")
        assert len(next_urls) <= 1
        if next_urls:
            next_url = next_urls[0]
        else:
            next_url = None
        crawl_index_page(context, doc)
