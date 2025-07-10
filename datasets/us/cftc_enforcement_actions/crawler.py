from typing import Optional, List, Literal
from pydantic import BaseModel, Field

from lxml.etree import tostring

from zavod.context import Context
from zavod import helpers as h
from zavod.shed.gpt import run_typed_text_prompt
from zavod.stateful.extraction import extract_items


class Associate(BaseModel):
    name: str
    address: Optional[str] = Field(
        default=None,
        description=("The address or even just the district/state of the defendant."),
    )
    relationship: Optional[str] = Field(
        default=None,
        description=(
            "The relationship of the associate to the defendant e.g. owner, officer, etc."
        ),
    )


# Heads-up associates seems to be a bit different each time making potentially
# unnecessary invalidations when the cache expires.
class Defendant(BaseModel):
    schema: Literal["Person", "Company", "LegalEntity"]
    name: str
    aliases: Optional[List[str]] = []
    address: Optional[str] = Field(
        default=None,
        description=("The address or even just the district/state of the defendant."),
    )
    country: Optional[str] = None
    owners: List[str] = Field(
        default=[], description=("The names of the owners of a Company defendant.")
    )
    associates: List[Associate] = Field(
        default=[],
        description=(
            "The names of the associates of a defendant excluding relief defendants. Prefer listing people under a company rather than companies under people."
        ),
    )
    authority: Optional[str] = None


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = """
Extract the defendants or entities added to the Red List in the attached article.
Leave out any relief defendants. Leave fields null or lists empty if values are not
present in the source text.
"""


def crawl_enforcement_action(context: Context, date: str, url: str) -> None:
    doc = context.fetch_html(url, cache_days=30)
    article = doc.xpath(".//article")[0]
    html = tostring(article, pretty_print=True).decode("utf-8")
    result = run_typed_text_prompt(context, PROMPT, html, response_type=Defendants)
    accepted_result = extract_items(
        context,
        key=url,
        source_value=html,
        source_content_type="text/html",
        source_label="Enforcement Action Notice",
        orig_extraction_data=result,
        source_url=url,
    )
    if not accepted_result:
        return
    for item in accepted_result.defendants:
        entity = context.make(item.schema)
        entity.id = context.make_id(item.name, item.address, item.country)
        entity.add("name", item.name)
        entity.add("address", item.address)
        entity.add("country", item.country)
        entity.add("alias", item.aliases)
        context.emit(entity)


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
