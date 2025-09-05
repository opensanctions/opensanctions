from lxml.html import HtmlElement, fromstring, tostring
from pydantic import BaseModel, Field
from rigour.mime.types import HTML
from typing import List, Literal

from zavod import Context
from zavod import helpers as h
from zavod.shed.gpt import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    Review,
    request_review,
    get_review,
    model_hash,
    html_to_text_hash,
)

Schema = Literal["Person", "Organization", "Company", "LegalEntity", "Vessel"]
NAME_XPATH = "//h2[@class='uswds-page-title']"
CONTENT_XPATH = "//article[@class='entity--type-node']"
DATE_XPATH = "//article[@class='entity--type-node']//time[@class='datetime']/@datetime"

MODEL_VERSION = 1
MIN_MODEL_VERSION = 1
MAX_TOKENS = 16384  # gpt-4o supports at most 16384 completion tokens

something_changed = False

schema_field = Field(
    description=(
        "- 'Person', if the name refers to an individual human."
        "- 'Vessel', if the name refers to a ship or vessel."
        "- 'Organization', for groups like terrorist groups, cartels or government bodies."
        "- 'LegalEntity', for entities when it is unclear if the entity is a person or company."
        "NEVER invent new schema labels."
    )
)


class Designee(BaseModel):
    entity_schema: Schema = schema_field
    name: str
    aliases: List[str] = []
    nationality: List[str] = []
    imo: List[str] = []
    country: List[str] = []
    related_url: List[str] = []


class Designees(BaseModel):
    designees: List[Designee]


PROMPT = f"""
<task>
Extract sanctions designees, linked entities, and vessels from the OFAC press release in the attached article.
</task>

<strict_requirements>
- NEVER infer, assume, or generate values not directly stated in the source text
- Extract ONLY information explicitly written in the article
- If data is not provided for a field, leave it empty
- Do not create or modify URLs
- Do not invent any country information
</strict_requirements>

<exclusions>
EXCLUDE from extraction:
- US Treasury officials (e.g., Secretary, Under Secretary)
- US federal government entities (e.g., Department of Treasury, SEC, OFAC itself)
</exclusions>

<entity_classification>
When determining entity_schema:
- Available options: {schema_field.description}
- Use "Person" for individual human names
- Use "Organization" as default for entities without specified legal form (like: Inc, LLC, SA de CV)
</entity_classification>

<extraction_fields>
For each entity found, extract these fields:

1. **name**: The exact name as written in the article.
    - If the name is followed by an acronym in brackets, do not include this in the name.
   
2. **entity_schema**: Select from available schema types: {schema_field.description}
   
3. **aliases**: Alternative names or acronyms ONLY if they meet these criteria:
   - Must be explicitly stated as "also known as", "formerly", "aka", "fka", or similar. Include only the alias, not the "aka" prefix.
   - Must be a legitimate alternative name or widely recognized abbreviation
   - Include acronyms that are MORE descriptive than the primary name (e.g., "March 23 Movement" as an alias for "M23")
   - EXCLUDE subsets of the full name (e.g., "Smith" from "John Michael Smith")
   - EXCLUDE text reference aliases in brackets unless explicitly marked as aliases
   
4. **nationality**: For individuals ONLY - their stated nationality
   - Leave empty if not explicitly mentioned or if entity is not a Person
   
5. **imo**: International Maritime Organization number
   - For vessels ONLY when explicitly stated
   
6. **country**: Countries mentioned as:
   - Residence location
   - Registration location  
   - Operation location
   - MUST be explicitly stated, not inferred
   
7. **related_url**: URLs specifically associated with the entity
   - Link each URL only to its associated entity
   - Leave empty if no URL is provided
   - Do not modify or invent URLs
</extraction_fields>
"""


def source_changed(review: Review, article_element: HtmlElement) -> bool:
    """
    The key exists but the current source data looks different from the existing version
    in spite of heavy normalisation.
    """
    seen_element = fromstring(review.source_value)
    return html_to_text_hash(seen_element) != html_to_text_hash(article_element)


def get_or_request_review(context, html_part, article_key, url):
    review = get_review(context, Designees, article_key, MIN_MODEL_VERSION)
    if review is None:
        prompt_result = run_typed_text_prompt(
            context, PROMPT, html_part, Designees, MAX_TOKENS
        )
        review = request_review(
            context,
            article_key,
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
        prompt_result = run_typed_text_prompt(
            context, PROMPT, article_html, Designees, MAX_TOKENS
        )
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
    entity.id = context.make_id(item.name, item.country)
    entity.add("name", item.name, origin=DEFAULT_MODEL)
    nationality_prop = "nationality"
    if item.entity_schema != "Person":
        nationality_prop = "country"
    entity.add(nationality_prop, item.nationality, origin=DEFAULT_MODEL)
    if entity.schema == "Vessel":
        entity.add("imoNumber", item.imo, origin=DEFAULT_MODEL)
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
    article = context.fetch_html(url, cache_days=7)
    article.make_links_absolute(context.data_url)
    names = article.xpath(NAME_XPATH)
    assert len(names) == 1, f"Expected 1 title, got {len(names)}"
    article_name = names[0].text_content().strip()
    article_content = article.xpath(CONTENT_XPATH)
    for img in article.xpath(".//img"):
        if img.get("src").startswith("data:image"):
            img.getparent().remove(img)
    assert len(article_content) == 1
    article_element = article_content[0]
    date = article.xpath(DATE_XPATH)[0]
    article_html = tostring(article_element, pretty_print=True, encoding="unicode")
    assert all([article_name, article_html, date]), "One or more fields are empty"

    review = get_or_request_review(context, article_html, article_key=url, url=url)
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

    for item in review.extracted_data.designees:
        crawl_item(context, item, date, url, article_name)


def crawl(context: Context):
    page = 0
    while True:
        base_url = f"https://ofac.treasury.gov/press-releases?page={page}"
        doc = context.fetch_html(base_url, cache_days=1)
        doc.make_links_absolute(context.data_url)
        table = doc.xpath("//table[contains(@class, 'views-table')]")
        next_page = doc.xpath("//a[contains(@class, 'usa-pagination__next-page')]")
        if not table or not next_page:
            break
        assert len(table) == 1, "Expected exactly one table in the document"
        for row in h.parse_html_table(table[0]):
            links = h.links_to_dict(row.pop("press_release_link"))
            url = next(iter(links.values()))
            # Filter out unwanted download/media links
            if "/news/press-releases/" not in url:
                continue  # skip this row
            if "/index.php/" in url:
                url = url.replace("/index.php/", "/")
            crawl_press_release(context, url)
        page += 1
        assert page < 200

    # FIXME: This is different from enforcement lists in that it's really just supporting
    # information; so it might be more OK to allow partial emit. Turning this on to create
    # something to enrich on. - FL Sep 3, 2025

    # assert_all_accepted(context)
    # global something_changed
    # msg = "See what changed to determine whether to trigger re-review."
    # assert not something_changed, msg
