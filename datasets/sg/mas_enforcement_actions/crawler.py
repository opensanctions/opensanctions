from typing import List, Literal, Optional

from pydantic import BaseModel, Field
from zavod.extract.llm import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    HtmlSourceValue,
    assert_all_accepted,
    review_extraction,
)

from zavod import Context
from zavod import helpers as h

Schema = Literal["Person", "Company", "LegalEntity"]

schema_field = Field(
    description=(
        "- 'Person', if the name refers to an individual."
        "- 'Company', if the name refers to a company or organization."
        "- 'LegalEntity', when unclear if the entity is a person or company."
        "Never invent new schema labels."
    )
)


class Defendant(BaseModel):
    name: str
    entity_schema: Schema = schema_field
    aliases: List[str] = []
    nationality: List[str] = []
    country: List[str] = []
    related_url: List[str] = []


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = f"""
<task>
Extract the defendants and related entities named in the attached Monetary Authority
of Singapore enforcement action.
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
- Singapore government institutions and officials (e.g., MAS)
</exclusions>

<entity_classification>
When determining entity_schema:
- Available options: {schema_field.description}
</entity_classification>

<extraction_fields>
For each entity found, extract these fields:

1. **name**: The exact name as written in the article, excluding any titles.
    - If the name is followed by an acronym or alias in brackets, DO NOT include this in the name.
    - NEVER include the honorific/courtesy title (e.g. Ms, Mr) as a part of the name.
      <example>Ms Jane Doe (JD)</example> <error>Ms Jane Doe</error>

2. **entity_schema**: Select from available schema types: {schema_field.description}

3. **aliases**: ONLY extract aliases that follow an explicit indication of an
    _alternative_ name, such as "also known as", "alias", "formerly", "aka", "fka".
    Otherwise the aliases field should just be an empty array. Acronyms or otherwise
    shortened forms of their name in parentheses used to refer to the entity in the
    rest of the article are NOT aliases.

4. **nationality**: For individuals ONLY - their stated nationality
   - Leave empty if not explicitly mentioned or if entity is not a Person

5. **country**: Countries mentioned as:
   - Residence location
   - Registration location
   - Operation location
   - MUST be explicitly stated, not inferred

6. **related_url**: URLs specifically associated with the entity
   - Link each URL only to its associated entity
   - Leave empty if no URL is provided
   - Do not modify or invent URLs
</extraction_fields>
"""


def crawl_item(
    context: Context,
    *,
    item: Defendant,
    date: str,
    url: str,
    article_name: str,
    action_type: Optional[str],
    origin: str,
) -> None:
    entity = context.make(item.entity_schema)
    entity.id = context.make_id(item.name, item.country)
    entity.add("name", item.name, origin=origin)
    nationality_prop = "nationality"
    if item.entity_schema != "Person":
        nationality_prop = "country"
    entity.add(nationality_prop, item.nationality, origin=origin)
    entity.add("country", item.country, origin=origin)
    entity.add("alias", item.aliases, origin=origin)
    entity.add("sourceUrl", item.related_url, origin=origin)
    entity.add("sourceUrl", url)

    article = h.make_article(context, url, title=article_name, published_at=date)
    documentation = h.make_documentation(context, entity, article)
    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "date", date)
    sanction.set("sourceUrl", url)
    sanction.add("status", action_type)

    context.emit(entity)
    context.emit(article)
    context.emit(documentation)
    context.emit(sanction)


def crawl_enforcement_action(
    context: Context, url: str, date: str, action_type: Optional[str]
) -> None:
    article = context.fetch_html(url, cache_days=7)
    article.make_links_absolute(context.data_url)
    article_el = h.xpath_elements(
        article, "//div[contains(@class, 'mas-section__banner-item')]", expect_exactly=1
    )[0]
    article_name_el = h.xpath_elements(article_el, "./h1", expect_exactly=1)
    article_name = h.element_text(article_name_el[0])

    source_value = HtmlSourceValue(
        key_parts=url, label="Enforcement Action", element=article_el, url=url
    )
    prompt_result = run_typed_text_prompt(
        context, PROMPT, source_value.value_string, Defendants
    )
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=prompt_result,
        origin=DEFAULT_MODEL,
    )

    if not review.accepted:
        return

    for item in review.extracted_data.defendants:
        crawl_item(
            context,
            item=item,
            date=date,
            url=url,
            article_name=article_name,
            action_type=action_type,
            origin=review.origin,
        )


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=7, absolute_links=True)
    table = h.xpath_elements(doc, "//table", expect_exactly=1)
    for row in h.parse_html_table(table[0]):
        links = h.links_to_dict(row.pop("title"))
        str_row = h.cells_to_str(row)
        date = str_row.pop("issue_date")
        # entities = str_row.pop("person_company")
        action_type = str_row.pop("action_type")
        # list of defendants is available in the 'person_company' field
        context.audit_data(str_row, ["person_company"])
        url = next(iter(links.values()))
        assert url is not None
        assert date is not None
        crawl_enforcement_action(context, url, date, action_type)

    assert_all_accepted(context, raise_on_unaccepted=False)
