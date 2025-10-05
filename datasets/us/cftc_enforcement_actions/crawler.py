from typing import Optional, List, Literal
from pydantic import BaseModel, Field
import re

from zavod.entity import Entity
from zavod.shed import enforcements

from lxml.html import HtmlElement

from zavod.context import Context
from zavod import helpers as h
from zavod.shed.gpt import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    HtmlSourceValue,
    LLMExtractionConfig,
    assert_all_accepted,
    observe_source_value,
    request_review,
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

CRAWLER_VERSION = 1

REGEX_RELEASE_ID = re.compile(r"(\w{2,8}-\w{2,4}[\w #-]*)$")

# Not extracting relationships for now because the results were inconsistent
# between GPT queries.

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
original_press_release_number_field = Field(
    description=(
        "The original press release number of the enforcement action notice."
        " When announcing charges, this is the press release number of the"
        " announcement. When announcing court orders or dropped charges,"
        " this is the reference to the original press release."
    )
)


class RelatedCompany(BaseModel):
    name: str
    relationship: str


class Defendant(BaseModel):
    entity_schema: Schema = schema_field
    name: str
    aliases: List[str] | None = []
    address: str | List[str] | None = address_field
    country: str | List[str] | None = []
    status: Status = status_field
    notes: Optional[str] = notes_field
    original_press_release_number: Optional[str] = original_press_release_number_field
    related_companies: List[RelatedCompany] = []


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = f"""
Extract the defendants or entities added to the Red List in the attached article.
NEVER include relief defendants.
NEVER infer, assume, or generate values that are not directly stated in the source text.

Trading/D.B.A. names which follow a person name but look like company can just be
aliases of the person. If the name is a person name, use `Person` as the entity_schema.

Specific fields:

- entity_schema: {schema_field.description}
- address: {address_field.description}
- country: Any countries explicitly associated with the defendant in the text. Leave empty if not explicitly stated.
- status: {status_field.description}
- notes: {notes_field.description}
- original_press_release_number: {original_press_release_number_field.description}
- related_companies: If the defendant is a person and a related company is mentioned in the source text, add it here.
    - relationship: Use text verbatim from the source. If it's ambiguous, e.g. "agents and owners", use that text exactly as it is, plural and all.
"""


def get_release_id(url: str) -> str:
    path_suffix = url.split("/")[-1]
    match = REGEX_RELEASE_ID.search(path_suffix)
    assert match, f"Invalid release ID: {path_suffix}"
    return match.group(1)


def fetch_article(context: Context, url: str) -> HtmlElement | None:
    # TODO: handle length limit
    if url == "https://www.cftc.gov/PressRoom/PressReleases/7274-15":
        return None
    # Try the article in the main page first.
    doc = context.fetch_html(url, cache_days=30, absolute_links=True)
    article_element = doc.xpath(".//article")[0]
    # All but one are HTML, not PDF.
    redirect_link = article_element.xpath(
        ".//div[contains(@class, 'press-release-open-link-pdf-link')]//a/@href"
    )
    if redirect_link and len(article_element.text_content()) > 200:
        context.log.warning("Has redirect link but isn't tiny.", url=url)
    if not redirect_link and len(article_element.text_content()) < 200:
        context.log.warning("Is tiny but doesn't have a redirect link.", url=url)
        return None
    # If no article in main page, try the redirect link.
    if redirect_link and len(article_element.text_content()) < 200:
        # TODO: handle PDF
        if redirect_link[0].endswith(".pdf"):
            context.log.warning("Has PDF redirect link.", url=url)
            return None
        article_element = context.fetch_html(redirect_link[0], cache_days=30)
    assert len(article_element.text_content()) > 200
    return article_element


def make_related_company(context: Context, name: str) -> Entity:
    entity = context.make("Company")
    entity.id = context.make_id(name)
    entity.add("name", name)
    return entity


def make_company_link(
    context: Context, entity: Entity, related_company: Entity, relationship: str
) -> Entity:
    link = context.make("UnknownLink")
    link.id = context.make_id("Related company", entity.id, related_company.id)
    link.add("subject", entity)
    link.add("object", related_company)
    link.add("role", relationship)
    return link


def get_title(article_element: HtmlElement) -> str:
    titles = article_element.xpath(".//h1")
    assert len(titles) == 2
    assert "Release Number" in titles[0].text_content()
    return titles[1].text_content()


def crawl_enforcement_action(context: Context, date: str, url: str) -> None:
    release_id = get_release_id(url)
    article_element = fetch_article(context, url)
    if article_element is None:
        return

    source_value = HtmlSourceValue(
        key_parts=release_id,
        label="Enforcement Action Notice",
        url=url,
        element=article_element,
    )
    extraction_config = LLMExtractionConfig(
        data_model=Defendants, llm_model=DEFAULT_MODEL, prompt=PROMPT
    )
    observation = observe_source_value(context, source_value, extraction_config)
    review = observation.review
    if observation.should_extract:
        prompt_result = h.prompt_for_review(context, extraction_config, source_value)
        review = request_review(
            context,
            source_value,
            extraction_config,
            orig_extraction_data=prompt_result,
            crawler_version=CRAWLER_VERSION,
        )

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

        # We try to link press releases that refer to an original press release number
        # back to the original press release by using that in the sanction key.

        # In practice often the entity ID and thus sanction ID differs because of
        # different levels of address details in the press releases.
        sanction = h.make_sanction(
            context,
            entity,
            key=item.original_press_release_number or release_id,
        )
        h.apply_date(sanction, "date", date)
        sanction.set("sourceUrl", url)
        sanction.add("status", item.status, origin=DEFAULT_MODEL)
        sanction.add("summary", item.notes, origin=DEFAULT_MODEL)
        sanction.add("authorityId", release_id)
        sanction.add(
            "authorityId", item.original_press_release_number, origin=DEFAULT_MODEL
        )

        for related_company in item.related_companies:
            related_company_entity = make_related_company(context, related_company.name)
            link = make_company_link(
                context, entity, related_company_entity, related_company.relationship
            )
            context.emit(related_company_entity)
            context.emit(link)

        article = h.make_article(
            context, url, title=get_title(article_element), published_at=date
        )
        documentation = h.make_documentation(context, entity, article)

        context.emit(entity)
        context.emit(sanction)
        context.emit(article)
        context.emit(documentation)


def crawl_index_page(context: Context, doc) -> bool:
    """Returns false if we should stop crawling."""
    table_xpath = ".//div[contains(@class, 'view-content')]//table"
    tables = doc.xpath(table_xpath)
    assert len(tables) == 1
    for row in h.parse_html_table(tables[0]):
        enforcement_date = h.element_text(row["date"])
        if not enforcements.within_max_age(context, enforcement_date):
            return False
        action_cell = row["enforcement_actions"]
        # Remove related links so we can assert that there's one key link
        for ul in action_cell.findall(".//ul"):
            ul.getparent().remove(ul)
        urls = action_cell.xpath(".//a/@href")
        assert len(urls) == 1
        url = urls[0]
        crawl_enforcement_action(context, enforcement_date, url)
    return True


def crawl(context: Context) -> None:
    next_url: Optional[str] = context.data_url
    while next_url:
        doc = context.fetch_html(next_url, absolute_links=True)
        next_urls = doc.xpath(".//a[@rel='next']/@href")
        assert len(next_urls) <= 1
        if next_urls:
            next_url = next_urls[0]
        else:
            next_url = None
        if not crawl_index_page(context, doc):
            break

    assert_all_accepted(context)
