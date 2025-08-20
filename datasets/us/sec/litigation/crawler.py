from typing import Optional, List, Literal
import re

from lxml.html import HtmlElement, fromstring, tostring
from pydantic import BaseModel, Field
from rigour.mime.types import HTML

from zavod.shed import enforcements
from zavod.context import Context
from zavod import helpers as h
from zavod.shed.gpt import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    Review,
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
    "Final consent judgement",
    "Consent judgement",
    "Supplemental consent order",
    "Other",
]

MODEL_VERSION = 1
MIN_MODEL_VERSION = 1
REGEX_RELEASE_ID = re.compile(r"^lr-(\d{4,})$")

something_changed = False


positions_field = Field(
    default=[],
    description=(
        (
            "The positions held by the person for an entity who is a person. "
            "Populate this precisely as listed in the text when the text indicates "
            "the job role of a person, otherwise leave empty. Include the word 'former'"
            "if that is indicated in the text."
        )
    ),
)
schema_field = Field(
    description="Use LegalEntity if it isn't clear whether the entity is a person or a company."
)
address_field = Field(
    default=[],
    description=("The addresses or even just the districts/states of the defendant."),
)
status_field = Field(
    description=(
        "The status of the enforcement action notice."
        " If `Other`, add the text used as the status in the source to `notes`."
    )
)
notes_field = Field(default=None, description=("Only used if `status` is `Other`."))


class Defendant(BaseModel):
    entity_schema: Schema = schema_field
    name: str
    aliases: List[str] = []
    positions: List[str] = positions_field
    address: List[str] = address_field
    country: List[str] = []
    status: Status = status_field
    notes: Optional[str] = notes_field


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = f"""
Extract the defendants subject to litigation announced in the attached article.
NEVER include relief defendants.
NEVER infer, assume, or generate values that are not directly stated in the source text.

Specific fields:

- entity_schema: {schema_field.description}
- positions: {positions_field.description}
- address: {address_field.description}
- country: Any countries the entity is indicated to reside, operate, or have been born or registered in.
- status: {status_field.description}
- notes: {notes_field.description}
"""


def strip_non_body_content(article_element: HtmlElement) -> None:
    for el in article_element.xpath(
        ".//*[text()='U.S. SECURITIES AND EXCHANGE COMMISSION']"
    ):
        el.getparent().remove(el)


def get_title(article_element: HtmlElement) -> str:
    titles = article_element.xpath(".//h1")
    assert len(titles) == 1
    return titles[0].text_content()


def get_case_numbers(article_element: HtmlElement) -> List[str]:
    case_numbers = []
    for h3 in article_element.xpath(".//h3"):
        h3_text = h3.text_content().strip()
        if "No." in h3_text and "v." in h3_text:
            case_numbers.append(h3_text)
    return case_numbers


def get_release_id(url: str) -> str:
    path_suffix = url.split("/")[-1]
    match = REGEX_RELEASE_ID.search(path_suffix)
    assert match, f"Invalid release ID: {path_suffix}"
    return match.group(1)


def source_changed(review: Review, article_element: HtmlElement) -> bool:
    """
    The key exists but the current source data looks different from the existing version
    in spite of heavy normalisation.
    """
    seen_element = fromstring(review.source_value)
    return html_to_text_hash(seen_element) != html_to_text_hash(article_element)


def check_something_changed(
    context: Context,
    review: Review,
    article_html: str,
    article_element: HtmlElement,
) -> bool:
    """
    Returns True if the source content has changed.

    In that case it also reprompts to log whether the extracted data has changed.
    """
    if source_changed(review, article_element):
        prompt_result = run_typed_text_prompt(context, PROMPT, article_html, Defendants)
        if model_hash(prompt_result) == model_hash(review.orig_extraction_data):
            context.log.warning(
                "The source content has changed but the extracted data has not",
                url=review.source_url,
                seen_source_value=review.source_value,
                new_source_value=article_html,
            )
        else:
            # A new extraction result looks different from the known original extraction
            context.log.warning(
                "The extracted data has changed",
                url=review.source_url,
                orig_extracted_data=review.orig_extraction_data.model_dump(),
                prompt_result=prompt_result.model_dump(),
            )
        return True
    else:
        return False


def crawl_release(
    context: Context, date: str, url: str, see_also_urls: List[str]
) -> None:

    doc = context.fetch_html(url, cache_days=15)
    doc.make_links_absolute(url)
    article_xpath = (
        ".//div[contains(@class, 'node-details-layout__main-region__content')]"
    )
    article_element = doc.xpath(article_xpath)[0]
    case_numbers = get_case_numbers(article_element)
    strip_non_body_content(article_element)
    article_html = tostring(article_element, pretty_print=True).decode("utf-8")
    release_id = get_release_id(url)
    review = get_review(context, Defendants, release_id, MIN_MODEL_VERSION)
    if review is None:
        prompt_result = run_typed_text_prompt(context, PROMPT, article_html, Defendants)
        review = request_review(
            context,
            release_id,
            article_html,
            HTML,
            "Litigation Release",
            url,
            prompt_result,
            MODEL_VERSION,
        )

    if check_something_changed(context, review, article_html, article_element):
        # In the first iteration, we're being super conservative and rejecting
        # export if the source content has changed regardless of whether the
        # extraction result has changed. If we see this happening and we see that
        # the extraction result reliably identifies real data changes, we can
        # relax this to only reject if the extraction result has changed.

        # Similarly if we see that broad markup changes don't trigger massive
        # re-reviews but legitimate changes are reliably detected, we can allow
        # it to automatically request re-reviews upon extraction changes.
        global something_changed
        something_changed = True
        return

    if not review.accepted:
        return

    for item in review.extracted_data.defendants:
        entity = context.make(item.entity_schema)
        entity.id = context.make_id(item.name, item.address, item.country)
        entity.add("name", item.name, origin=DEFAULT_MODEL)
        if item.address != item.country:
            entity.add("address", item.address, origin=DEFAULT_MODEL)
        entity.add("country", item.country, origin=DEFAULT_MODEL)
        entity.add("alias", item.aliases, origin=DEFAULT_MODEL)
        entity.add("topics", "reg.action")

        # Distinct actions should be distinct sanctions, while different releases
        # about the same action should end up in the same sanction. We use the list of
        # case numbers for this. There is normally only one case number per release,
        # but sometimes more, in which case sanction id might not be consistent between releases.
        # The id might also differ if the entity id differs based on differing address details
        # between releases.
        sanction = h.make_sanction(context, entity, key=case_numbers)
        h.apply_date(sanction, "date", date)
        sanction.add("sourceUrl", url)
        sanction.add("sourceUrl", see_also_urls)
        sanction.add("status", item.status, origin=DEFAULT_MODEL)
        sanction.add("summary", item.notes, origin=DEFAULT_MODEL)
        sanction.add("authorityId", release_id)

        article = h.make_article(context, url, title=get_title(doc), published_at=date)
        documentation = h.make_documentation(context, entity, article)

        context.emit(entity)
        context.emit(sanction)
        context.emit(article)
        context.emit(documentation)


def crawl_index_page(context: Context, doc) -> bool:
    """Returns false if we should stop crawling."""
    table_xpath = ".//table[contains(@class, 'views-view-table')]"
    tables = doc.xpath(table_xpath)
    assert len(tables) == 1
    for row in h.parse_html_table(tables[0]):
        enforcement_date = row["date_sort_descending"].text_content().strip()
        if not enforcements.within_max_age(context, enforcement_date):
            return False
        action_cell = row["respondents"]
        action_link_xpath = (
            ".//div[contains(@class, 'release-view__respondents')]//a/@href"
        )
        urls = action_cell.xpath(action_link_xpath)
        assert len(urls) == 1
        url = urls[0]
        if "see also" in action_cell.text_content().lower():
            see_also_xpath = ".//span[contains(@class, 'media--view-mode-release-see-also-files')]//a/@href"
            see_also_urls = action_cell.xpath(see_also_xpath)
            assert len(see_also_urls) > 0
        else:
            see_also_urls = []
        crawl_release(context, enforcement_date, url, see_also_urls)
    return True


def crawl(context: Context) -> None:
    next_url: Optional[str] = context.data_url
    while next_url:
        doc = context.fetch_html(next_url)
        doc.make_links_absolute(next_url)
        next_urls = doc.xpath(
            ".//a[contains(@class, 'usa-pagination__next-page')]/@href"
        )
        assert len(next_urls) <= 1
        if next_urls:
            next_url = next_urls[0]
        else:
            next_url = None
        if not crawl_index_page(context, doc):
            break

    assert_all_accepted(context)
    global something_changed
    assert (
        not something_changed
    ), "See what changed to determine whether to trigger re-review."
