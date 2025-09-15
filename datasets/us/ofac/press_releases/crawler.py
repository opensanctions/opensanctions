from lxml.html import tostring
from pydantic import BaseModel, Field
from rigour.mime.types import HTML
from typing import List, Literal

from zavod import Context
from zavod import helpers as h
from zavod.shed.gpt import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import Review, request_review, get_review

Schema = Literal["Person", "Organization", "Company", "LegalEntity", "Vessel"]

MODEL_VERSION = 1
MIN_MODEL_VERSION = 1
MAX_TOKENS = 16384  # gpt-4o supports at most 16384 completion tokens

schema_field = Field(
    description=(
        "- 'Person', if the name refers to an individual human."
        "- 'Vessel', if the name refers to a ship or vessel."
        "- 'Company', for entities with a clear legal form (e.g., Inc, LLC, SA de CV)."
        "- 'Organization', for groups like terrorist groups, cartels or government bodies."
        "- 'LegalEntity', when it is unclear if the entity is a person, company or organization."
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
</entity_classification>

<extraction_fields>
For each entity found, extract these fields:

1. **name**: The exact name as written in the article.
    - If the name is followed by an acronym or alias in brackets, DO NOT include this in the name.
   
2. **entity_schema**: Select from available schema types: {schema_field.description}
   
3. **aliases**: Alternative names ONLY if they meet these criteria:
   - Must be explicitly stated as "also known as", "alias", "formerly", "aka", "fka", or similar. Include ONLY the alias, not the "aka" prefix.
   - An alias MUST NOT be a simple abbreviation, only consisting of the last name, first name, family name of a person.
     <example>John Smith (Smith)</example> <error>Smith</error>
   - An alias MUST NOT be just the name of a company without the legal form.
     <example>Acme Corporation (Acme)</example> <error>Acme</error>
     <example>Acme, Ltd (Acme)</example> <error>Acme</error>

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


def get_or_request_review(context, html_part, article_key, url) -> Review:
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
    names = article.findall(".//h2[@class='uswds-page-title']")
    assert len(names) == 1, f"Expected 1 title, got {len(names)}"
    article_name = h.element_text(names[0])
    article_content = article.findall("//article[@class='entity--type-node']")
    for img in article.findall(".//img"):
        img_src = img.get("src")
        if img_src is None or img_src.startswith("data:image"):
            img_parent = img.getparent()
            if img_parent is not None:
                img_parent.remove(img)
    assert len(article_content) == 1
    article_element = article_content[0]
    date = article_element.findall(".//time[@class='datetime']/@datetime")[0]
    article_html = tostring(article_element, pretty_print=True, encoding="unicode")
    assert all([article_name, article_html, date]), "One or more fields are empty"

    review = get_or_request_review(context, article_html, article_key=url, url=url)

    if review is None or not review.accepted:
        return

    for item in review.extracted_data.designees:
        crawl_item(context, item, date, url, article_name)


def crawl(context: Context):
    page = 0
    while True:
        base_url = f"https://ofac.treasury.gov/press-releases?page={page}"
        doc = context.fetch_html(base_url, cache_days=1)
        doc.make_links_absolute(context.data_url)
        table = doc.findall("//table[contains(@class, 'views-table')]")
        next_page = doc.findall("//a[contains(@class, 'usa-pagination__next-page')]")
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
