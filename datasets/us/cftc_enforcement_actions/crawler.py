from typing import Optional, List, Literal
from pydantic import BaseModel, Field
import re

from lxml.html import fromstring, tostring

from zavod.context import Context
from zavod import helpers as h
from zavod.shed.gpt import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    assert_all_accepted,
    request_review,
    get_review,
    model_hash,
    html_to_text_hash,
)

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


PROMPT = """
Extract the defendants or entities added to the Red List in the attached article.
Leave out any relief defendants. Leave fields null or lists empty if values are not
present in the source text.

Trading/D.B.A. names which follow a person name but look like company can just be
aliases of the person.
"""

REGEX_RELEASE_ID = re.compile(r"(\w{2,8}-\w{2,4}[\w #-]*)$")

something_changed = False


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


def get_release_id(url: str) -> str:
    path_suffix = url.split("/")[-1]
    match = REGEX_RELEASE_ID.search(path_suffix)
    assert match, f"Invalid release ID: {path_suffix}"
    return match.group(1)


def crawl_enforcement_action(context: Context, date: str, url: str) -> None:
    # TODO: handle length limit
    if url == "https://www.cftc.gov/PressRoom/PressReleases/7274-15":
        return
    # Try the article in the main page first.
    doc = context.fetch_html(url, cache_days=30)
    doc.make_links_absolute(url)
    article = doc.xpath(".//article")[0]
    # All but one are HTML, not PDF.
    redirect_link = article.xpath(
        ".//div[contains(@class, 'press-release-open-link-pdf-link')]//a/@href"
    )
    if redirect_link and len(article.text_content()) > 200:
        context.log.warning("Has redirect link but isn't tiny.", url=url)
    if not redirect_link and len(article.text_content()) < 200:
        context.log.warning("Is tiny but doesn't have a redirect link.", url=url)
        return
    # If no article in main page, try the redirect link.
    if redirect_link and len(article.text_content()) < 200:
        # TODO: handle PDF
        if redirect_link[0].endswith(".pdf"):
            context.log.warning("Has PDF redirect link.", url=url)
            return
        article = context.fetch_html(redirect_link[0], cache_days=30)
    assert len(article.text_content()) > 200

    article_html_string = tostring(article, pretty_print=True).decode("utf-8")
    release_id = get_release_id(url)
    review = get_review(context, Defendants, release_id, MIN_MODEL_VERSION)
    if review is None:
        # The key doesn't exist or we've bumped MIN_MODEL_VERSION to require a re-review
        prompt_result = run_typed_text_prompt(
            context, PROMPT, article_html_string, Defendants
        )
        review = request_review(
            context,
            release_id,
            article_html_string,
            "text/html",
            "Enforcement Action Notice",
            url,
            prompt_result,
            MODEL_VERSION,
        )
    seen_article = fromstring(review.source_value)
    if html_to_text_hash(seen_article) != html_to_text_hash(article):
        # The key exists but the current source data looks different from the existing version
        # in spite of heavy normalisation.

        # In the first iteration, we're being super conservative and rejecting
        # export if the source content has changed regardless of whether the
        # extraction result has changed. If we see this happening and we see that
        # the extraction result reliably identifies real data changes, we can move
        # this into the clause where the extraction result has changed.
        global something_changed
        something_changed = True

        prompt_result = run_typed_text_prompt(
            context, PROMPT, article_html_string, Defendants
        )
        if model_hash(prompt_result) != model_hash(review.orig_extraction_data):
            # A new extraction result looks different from the known original extraction
            context.log.warning(
                "The extracted data has changed",
                url=url,
                orig_extracted_data=review.orig_extraction_data.model_dump(),
                prompt_result=prompt_result.model_dump(),
            )
        else:
            context.log.warning(
                "The source content has changed but the extracted data has not",
                url=url,
                seen_source_value=review.source_value,
                new_source_value=article_html_string,
            )
            return

    if not review.accepted:
        return

    for item in review.extracted_data.defendants:
        entity = context.make(item.entity_schema)
        entity.id = context.make_id(item.name, item.address, item.country)
        entity.add("name", item.name, origin=DEFAULT_MODEL)
        entity.add("address", item.address, origin=DEFAULT_MODEL)
        entity.add("country", item.country, origin=DEFAULT_MODEL)
        entity.add("alias", item.aliases, origin=DEFAULT_MODEL)
        entity.add("topics", "reg.action")

        # We try to link press releases that refer to an original press release number
        # back to the original press release by using that in the sanction key.

        # In practice often the entity ID and thus sanction ID differs because of
        # different levels of address details in the press releases.
        sanction = h.make_sanction(
            context,
            entity,
            key=item.original_press_release_number or release_id,
        )
        h.apply_date(sanction, "date", date.strip())
        sanction.set("sourceUrl", url)
        sanction.add("status", item.status, origin=DEFAULT_MODEL)
        sanction.add("summary", item.notes, origin=DEFAULT_MODEL)
        sanction.add("authorityId", release_id)
        sanction.add(
            "authorityId", item.original_press_release_number, origin=DEFAULT_MODEL
        )

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
        doc = context.fetch_html(next_url, cache_days=30)
        doc.make_links_absolute(next_url)
        next_urls = doc.xpath(".//a[@rel='next']/@href")
        assert len(next_urls) <= 1
        if next_urls:
            next_url = next_urls[0]
        else:
            next_url = None
        crawl_index_page(context, doc)

    assert_all_accepted(context)
    global something_changed
    assert (
        not something_changed
    ), "See what changed to determine whether to trigger re-review."
