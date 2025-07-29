from typing import Optional, List, Literal
from pydantic import BaseModel, Field

from lxml.etree import tostring

from zavod.context import Context
from zavod import helpers as h
from zavod.shed.gpt import run_typed_text_prompt
from zavod.stateful.review import request_review, get_review

Schema = Literal["Person", "Company", "LegalEntity"]
Status = Literal[
    "Filed",
    "Dismissed",
    "Settled",
    "Default judgement",
    "Final judgement",
    "Supplemental consent order",
    "Other",
]

MODEL_VERSION = 1
MIN_MODEL_VERSION = 1


# Not extracting relationships for now because the results were inconsistent
# between GPT queries.
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
    original_press_release_number: Optional[str] = Field(
        default=None,
        description=(
            "The original press release number of the enforcement action notice."
            " When announcing charges, this is the press release number of the"
            " announcement. When announcing court orders or dropped charges,"
            " this is the reference to the original press release."
        ),
    )


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = """
Extract the defendants or entities added to the Red List in the attached article.
Leave out any relief defendants. Leave fields null or lists empty if values are not
present in the source text.

Trading/D.B.A. names looking like company rather than person names should be extracted
as companies, not aliases of a person.
"""


def crawl_enforcement_action(context: Context, date: str, url: str) -> None:
    doc = context.fetch_html(url, cache_days=30)
    article = doc.xpath(".//article")[0]
    html = tostring(article, pretty_print=True).decode("utf-8")
    review = get_review(context, Defendants, url, MIN_MODEL_VERSION)
    if review is None:
        prompt_result = run_typed_text_prompt(
            context, PROMPT, html, response_type=Defendants
        )
        review = request_review(
            context,
            url,
            html,
            "text/html",
            "Enforcement Action Notice",
            url,
            prompt_result,
            MODEL_VERSION,
        )
    if not review.accepted:
        return

    for item in review.extracted_data.defendants:
        entity = context.make(item.entity_schema)
        entity.id = context.make_id(item.name, item.address, item.country)
        entity.add("name", item.name)
        entity.add("address", item.address)
        entity.add("country", item.country)
        entity.add("alias", item.aliases)
        if item.status != "Dismissed":
            entity.add("topics", "reg.action")

        press_release_num = doc.xpath(".//h1[contains(@class, 'press-release-title')]")[
            0
        ].text_content()
        press_release_num = press_release_num.replace("Release Number", "").strip()
        # We try to link press releases that refer to an original press release number
        # back to the original press release.
        # In practice often the entity ID differs initially because of different levels
        # of address details in the press release.
        sanction = h.make_sanction(
            context, entity, key=item.original_press_release_number
        )
        h.apply_date(sanction, "date", date.strip())
        sanction.add("sourceUrl", url)
        sanction.add("status", item.status)
        sanction.add("summary", item.notes)
        sanction.add("authorityId", press_release_num)
        sanction.add("authorityId", item.original_press_release_number)

        context.emit(entity)
        context.emit(sanction)


def crawl_index_page(context: Context, doc) -> None:
    table_xpath = ".//div[contains(@class, 'view-content')]//table"
    tables = doc.xpath(table_xpath)
    assert len(tables) == 1
    for row in h.parse_html_table(tables[0]):
        date = row["date"].text_content()
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
        doc = context.fetch_html(next_url)
        doc.make_links_absolute(next_url)
        next_urls = doc.xpath(".//a[@rel='next']/@href")
        assert len(next_urls) <= 1
        if next_urls:
            next_url = next_urls[0]
        else:
            next_url = None
        crawl_index_page(context, doc)
