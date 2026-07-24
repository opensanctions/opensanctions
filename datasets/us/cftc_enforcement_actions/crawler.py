import re
from typing import Literal

from pydantic import BaseModel, Field
from zavod.context import Context
from zavod.entity import Entity
from zavod.extract.llm import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    HtmlSourceValue,
    assert_all_accepted,
    review_extraction,
)
from zavod.util import Element

from zavod import helpers as h

Schema = Literal["Person", "Company", "LegalEntity"]

REGEX_RELEASE_ID = re.compile(r"(\w{2,8}-\w{2,4}[\w #-]*)$")

# Not extracting relationships for now because the results were inconsistent
# between GPT queries.


class RelatedCompany(BaseModel):
    name: str
    relationship: str


class Defendant(BaseModel):
    entity_schema: Schema = Field(
        description="Use LegalEntity if it isn't clear whether the entity is a person or a company."
    )
    name: str
    aliases: list[str] | None = []
    address: str | list[str] | None = Field(
        default=[],
        description="The addresses or even just the districts/states of the defendant.",
    )
    country: str | list[str] | None = []
    # status - was removed because it isn't consistently present in the source
    #          text in a way we can extract as a meaningful self-standing value.
    #          It remains in the json of old accepted values but is unused and
    #          not added for new entries.
    # notes - was just additional context to 'status'. Same fate.
    original_press_release_number: str | None = Field(
        description=(
            "The original press release number of the enforcement action notice."
            " When announcing charges, this is the press release number of the"
            " announcement. When announcing court orders or dropped charges,"
            " this is the reference to the original press release."
        )
    )
    related_companies: list[RelatedCompany] = []


class Defendants(BaseModel):
    defendants: list[Defendant]


PROMPT = f"""
Extract the defendants or entities added to the Red List in the attached article.
NEVER include relief defendants.
NEVER infer, assume, or generate values that are not directly stated in the source text.

Trading/D.B.A. names which follow a person name but look like company can just be
aliases of the person. If the name is a person name, use `Person` as the entity_schema.

Specific fields:

- entity_schema: {Defendant.model_fields["entity_schema"].description}
- address: {Defendant.model_fields["address"].description}
- country: Any countries explicitly associated with the defendant in the text. Leave empty if not explicitly stated.
- original_press_release_number: {Defendant.model_fields["original_press_release_number"].description}
- related_companies: If the defendant is a person and a related company is mentioned in the source text, add it here.
    - relationship: Use text verbatim from the source. If it's ambiguous, e.g. "agents and owners", use that text exactly as it is, plural and all.
"""


def get_release_id(url: str) -> str:
    path_suffix = url.split("/")[-1]
    match = REGEX_RELEASE_ID.search(path_suffix)
    assert match, f"Invalid release ID: {path_suffix}"
    return match.group(1)


def fetch_article(context: Context, url: str) -> Element | None:
    # TODO: handle length limit
    if url == "https://www.cftc.gov/PressRoom/PressReleases/7274-15":
        return None
    # Try the article in the main page first.
    doc = context.fetch_html(url, cache_days=30, absolute_links=True)
    article_element = h.xpath_elements(doc, ".//article")[0]
    # All but one are HTML, not PDF.
    redirect_link = h.xpath_strings(
        article_element,
        ".//div[contains(@class, 'press-release-open-link-pdf-link')]//a/@href",
    )
    if redirect_link and len(h.element_text(article_element)) > 200:
        context.log.warning("Has redirect link but isn't tiny.", url=url)
    if not redirect_link and len(h.element_text(article_element)) < 200:
        context.log.warning("Is tiny but doesn't have a redirect link.", url=url)
        return None
    # If no article in main page, try the redirect link.
    if redirect_link and len(h.element_text(article_element)) < 200:
        # TODO: handle PDF
        if redirect_link[0].endswith(".pdf"):
            context.log.warning("Has PDF redirect link.", url=url)
            return None
        article_element = context.fetch_html(redirect_link[0], cache_days=30)
    assert len(h.element_text(article_element)) > 200
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


def get_title(article_element: Element) -> str:
    titles = h.xpath_elements(article_element, ".//h1")
    assert len(titles) == 2
    assert "Release Number" in h.element_text(titles[0])
    return h.element_text(titles[1])


def crawl_enforcement_action(context: Context, date: str, url: str) -> None:
    release_id = get_release_id(url)
    article_element = fetch_article(context, url)
    if article_element is None:
        return

    source_value = HtmlSourceValue(
        key_parts=release_id,
        label="Enforcement Action Notice",
        element=article_element,
        url=url,
    )
    prompt_result = run_typed_text_prompt(
        context, PROMPT, response_type=Defendants, string=source_value.value_string
    )
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=prompt_result,
        origin=DEFAULT_MODEL,
    )

    for item in review.extracted_data.defendants:
        entity = context.make(item.entity_schema)
        # make_id takes str | None; item.address/item.country are lists but must
        # stay in the key unchanged to avoid re-keying existing entities.
        entity.id = context.make_id(item.name, item.address, item.country)  # type: ignore[arg-type]
        entity.add("name", item.name, origin=review.origin)
        if item.address != item.country:
            entity.add("address", item.address, origin=review.origin)
        entity.add("country", item.country, origin=review.origin)
        entity.add("alias", item.aliases, origin=review.origin)
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
        sanction.add("authorityId", release_id)
        sanction.add(
            "authorityId", item.original_press_release_number, origin=review.origin
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


def crawl_index_page(context: Context, doc: Element) -> bool:
    """Returns false if we should stop crawling."""
    table_xpath = ".//div[contains(@class, 'view-content')]//table"
    table = h.xpath_element(doc, table_xpath)
    for row in h.parse_html_table(table):
        enforcement_date = h.element_text(row["date"])
        if not h.within_max_age(context, enforcement_date):
            return False
        action_cell = row["enforcement_actions"]
        # Remove related links so we can assert that there's one key link
        for ul in action_cell.findall(".//ul"):
            parent = ul.getparent()
            assert parent is not None
            parent.remove(ul)
        urls = h.xpath_strings(action_cell, ".//a/@href")
        assert len(urls) == 1
        url = urls[0]
        crawl_enforcement_action(context, enforcement_date, url)
    return True


def crawl(context: Context) -> None:
    next_url: str | None = context.data_url
    while next_url:
        doc = context.fetch_html(next_url, absolute_links=True)
        next_urls = h.xpath_strings(doc, ".//a[@rel='next']/@href")
        assert len(next_urls) <= 1
        if next_urls:
            next_url = next_urls[0]
        else:
            next_url = None
        if not crawl_index_page(context, doc):
            break

    assert_all_accepted(context)
