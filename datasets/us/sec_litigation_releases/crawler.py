import re
from typing import Optional, List, Literal
from pydantic import BaseModel, Field

from lxml.etree import tostring

from zavod.context import Context
from zavod import helpers as h
from zavod.shed.gpt import run_typed_text_prompt
from zavod.stateful.review import get_accepted_data

Schema = Literal["Person", "Company", "LegalEntity"]
# TODO: Copied from CFTC for now. We should really get these right. What's the difference between "Settled" and "Consent order"? What happens to those that are no
Status = Literal[
    "Filed",
    "Dismissed",
    "Settled",
    "Default judgement",
    "Final judgement",
    "Supplemental consent order",
    "Other",
]


class Defendant(BaseModel):
    entity_schema: Schema = Field(
        description="Use LegalEntity if it isn't clear whether the entity is a person or a company."
    )
    name: str
    aliases: Optional[List[str]] = []
    address: Optional[str] = Field(
        default=None,
        description=("The address or even just the district/state of the defendant."),
    )
    country: Optional[str] = None
    status: Status = Field(
        description=(
            "The status of the enforcement action notice."
            " If `Other`, add the text used as the status in the source to `notes`."
        )
    )
    notes: Optional[str] = Field(
        default=None, description=("Only used if `status` is `Other`.")
    )


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = """
Extract the defendants or entities listed as defendants in the attached press release about a litigation.
Leave fields null or lists empty if values are not present in the source text.

Trading/D.B.A. names looking like company rather than person names should be extracted
as companies, not aliases of a person.
"""


def crawl_litigation_release(
    context: Context, *, date: str, release_number_subfield: str, url: str
) -> None:
    doc = context.fetch_html(url, cache_days=30)
    content = doc.xpath(".//div[@class='content-wrapper']")[0]
    html = tostring(content, pretty_print=True).decode("utf-8")
    prompt_result = run_typed_text_prompt(
        context, prompt=PROMPT, string=html, response_type=Defendants
    )
    result = get_accepted_data(
        context,
        key=url,
        source_value=html,
        source_content_type="text/html",
        source_label="Litigation Release",
        source_url=url,
        orig_extraction_data=prompt_result,
    )
    if not result:
        return

    release_numbers = [r.strip() for r in release_number_subfield.split(",")]
    assert len(release_numbers) >= 1
    assert re.match(r"LR-\d+", release_numbers[0])
    for release_number in release_numbers[1:]:
        # Sometimes additional identifiers such as "AAER-4571"
        assert re.match(r"\w+-\d+", release_number)

    for item in result.defendants:
        entity = context.make(item.entity_schema)
        entity.id = context.make_id(item.name, item.address, item.country)
        entity.add("name", item.name)
        entity.add("address", item.address)
        entity.add("country", item.country)
        entity.add("alias", item.aliases)
        if item.status != "Dismissed":
            entity.add("topics", "reg.action")

        # We try to link press releases that refer to an original press release number
        # back to the original press release.
        # In practice often the entity ID differs initially because of different levels
        # of address details in the press release.
        sanction = h.make_sanction(
            context,
            entity,
            # The LR-1234 is always first, use that
            key=release_numbers[0],
        )
        h.apply_date(sanction, "date", date.strip())
        sanction.add("sourceUrl", url)
        sanction.add("status", item.status)
        sanction.add("summary", item.notes)
        sanction.add("authorityId", release_numbers)

        context.emit(entity)
        context.emit(sanction)


def crawl_index_page(context: Context, doc) -> None:
    table_xpath = ".//div[contains(@class, 'view-content')]//table"
    tables = doc.xpath(table_xpath)
    assert len(tables) == 1
    for row in h.parse_html_table(tables[0]):
        date = row["date_sort_descending"].text_content()
        action_cell = row["respondents"]
        release_no = action_cell.xpath(
            ".//div[contains(@class, 'view-table_subfield_release_number')]//span[@class='view-table_subfield_value']/text()"
        )[0]
        # Remove related links so we can assert that there's one key link
        for a in action_cell.xpath(
            ".//div[contains(@class, 'view-table_subfield')]//div[contains(@class, 'view-table_subfield_value')]//a"
        ):
            a.getparent().remove(a)
        urls = action_cell.xpath(
            ".//div[contains(@class, 'release-view__respondents')]//a/@href"
        )
        assert len(urls) == 1
        url = urls[0]
        crawl_litigation_release(
            context, date=date, release_number_subfield=release_no, url=url
        )


def crawl(context: Context) -> None:
    next_url: Optional[str] = context.data_url
    while next_url:
        doc = context.fetch_html(next_url)
        doc.make_links_absolute(next_url)
        next_urls = doc.xpath(".//a[@rel='next']/@href")
        assert len(next_urls) <= 1
        # DEBUG
        next_url = None
        # next_url = next_urls[0] if next_urls else None
        crawl_index_page(context, doc)
