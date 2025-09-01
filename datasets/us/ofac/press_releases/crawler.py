from lxml.html import HtmlElement, fromstring, tostring
from pydantic import BaseModel, Field
from rigour.mime.types import HTML
from typing import List, Literal

from zavod import Context
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

# Rough conversion: 1 GPT token ≈ 4 chars English text
TOKEN_CHAR_RATIO = 4
MAX_TOKENS = 3000
MAX_CHARS = MAX_TOKENS * TOKEN_CHAR_RATIO


Schema = Literal["Person", "Company", "LegalEntity", "Vessel"]
NAME_XPATH = "//h2[@class='uswds-page-title']/text()"
CONTENT_XPATH = "//article[@class='entity--type-node']"
DATE_XPATH = "//article[@class='entity--type-node']//time[@class='datetime']/@datetime"

MODEL_VERSION = 1
MIN_MODEL_VERSION = 1

something_changed = False

schema_field = Field(
    description=(
        "- 'Person', if the name refers to an individual."
        "- 'Vessel', if the name refers to a ship or vessel."
        "- 'LegalEntity', for companies, organizations, or when unclear if the entity is a person or company."
        "Never invent new schema labels."
    )
)


class Designee(BaseModel):
    entity_schema: Schema = schema_field
    name: str
    aliases: List[str] = []
    nationality: List[str] = []
    country: List[str] = []
    related_url: List[str] = []


class Designees(BaseModel):
    designees: List[Designee]


PROMPT = f"""
Extract the designees, entities and vessels mentioned in OFAC press release in the attached article.
NEVER infer, assume, or generate values that are not directly stated in the source text.
If the name is a person name, use `Person` as the entity_schema.
Output each entity with these fields:
- name: Exact name as written in the article. If followed by an acronym in parentheses, store that acronym as an alias, not in the name.
- entity_schema: {schema_field.description}
- aliases: Other names or acronyms the entity is referred to in the article.
- nationality: Nationality of the designee if stated.
- country: Countries explicitly mentioned as residence, registration, or operation. Leave empty if not stated.
- related_url: URLs mentioned in the article specifically associated with the entity.  
  • If multiple URLs are present, link each one only to the entity it it is associated with.  
  • If no URL is provided for an entity, leave this field empty.  
  • Do not invent, infer, or alter URLs.
"""


def source_changed(review: Review, article_element: HtmlElement) -> bool:
    """
    The key exists but the current source data looks different from the existing version
    in spite of heavy normalisation.
    """
    seen_element = fromstring(review.source_value)
    return html_to_text_hash(seen_element) != html_to_text_hash(article_element)


def split_article_by_headers(article_el):
    """
    Splits an article into sections strictly at <h3> or <h4> headers.
    The content between headers is never cut.
    """
    sections = []
    buffer = []
    for elem in article_el.iter():
        # If we encounter a header and buffer is not empty, flush it
        if elem.tag.lower() in ("h3", "h4") and buffer:
            sections.append("".join(buffer))
            buffer = []
        # Append current element to buffer
        elem_html = tostring(elem, encoding="unicode", with_tail=True)
        buffer.append(elem_html)
    # Flush any remaining content in the buffer
    if buffer:
        sections.append("".join(buffer))
    return sections


def get_or_request_review(context, html_part, section_url, url):
    review = get_review(context, Designees, section_url, MIN_MODEL_VERSION)
    if review is None:
        prompt_result = run_typed_text_prompt(context, PROMPT, html_part, Designees)
        review = request_review(
            context,
            section_url,
            html_part,
            HTML,
            "Press Release",
            url,
            prompt_result,
            MODEL_VERSION,
        )
    return review


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
        prompt_result = run_typed_text_prompt(context, PROMPT, article_html, Designees)
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


def crawl_item(context, item, date, url, article_name):
    entity = context.make(item.entity_schema)
    entity.id = context.make_id(item.name, item.address, item.country)
    entity.add("name", item.name, origin=DEFAULT_MODEL)
    entity.add("nationality", item.nationality, origin=DEFAULT_MODEL)
    entity.add("country", item.country, origin=DEFAULT_MODEL)
    entity.add("alias", item.aliases, origin=DEFAULT_MODEL)
    entity.add("sourceUrl", item.related_url, origin=DEFAULT_MODEL)
    entity.add("sourceUrl", url)

    article = h.make_article(context, url, title=article_name, published_at=date)
    documentation = h.make_documentation(context, entity, article)

    context.emit(entity)
    context.emit(article)
    context.emit(documentation)


def crawl_press_release(context: Context, url: str) -> None:
    article = context.fetch_html(url, cache_days=1)
    article.make_links_absolute(context.data_url)
    if article is None:
        return
    article_name = article.xpath(NAME_XPATH)[0]
    article_content = article.xpath(CONTENT_XPATH)
    assert len(article_content) == 1
    article_element = article_content[0]
    date = article.xpath(DATE_XPATH)[0]
    article_html = tostring(article_element, pretty_print=True, encoding="unicode")
    assert all([article_name, article_html, date]), "One or more fields are empty"
    sections = (
        split_article_by_headers(article_element)
        if len(article_html) > MAX_CHARS
        else [article_html]
    )
    context.log.info(
        f"Article length {len(article_html)} chars — {'splitting into sections' if len(sections) > 1 else 'no split needed'}."
    )

    for i, section_html in enumerate(sections, 1):
        section_key = slugify(url, "section", i)  # distinguish reviews per section
        review = get_or_request_review(context, section_key, section_url, url)

        if check_something_changed(context, review, section_html, article_element):
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

        for item in review.extracted_data.designees:
            crawl_item(context, item, date, url, article_name)


def crawl(context: Context):
    page = 0
    while True:
        base_url = f"https://ofac.treasury.gov/press-releases?page={page}"
        doc = context.fetch_html(base_url, cache_days=1)
        doc.make_links_absolute(context.data_url)
        table = doc.xpath("//table[contains(@class, 'views-table')]")
        if not table:
            break
        assert len(table) == 1, "Expected exactly one table in the document"
        for row in h.parse_html_table(table[0]):
            links = h.links_to_dict(row.pop("press_release_link"))
            url = next(iter(links.values()))
            # Filter out unwanted download/media links
            if "/news/press-releases/" not in url:
                continue  # skip this row
            crawl_press_release(context, url)
        page += 1

    assert_all_accepted(context)
    global something_changed
    assert (
        not something_changed
    ), "See what changed to determine whether to trigger re-review."
