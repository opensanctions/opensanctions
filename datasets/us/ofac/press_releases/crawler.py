from typing import Literal

from lxml.html import tostring
from pydantic import BaseModel, Field
from zavod.extract.llm import run_typed_text_prompt
from zavod.stateful.review import (
    HtmlSourceValue,
    assert_all_accepted,
    review_extraction,
)

from zavod import Context
from zavod import helpers as h

Schema = Literal[
    "Person", "Organization", "Company", "LegalEntity", "Vessel", "Airplane"
]

LLM_MODEL = "gpt-6.1-sol"

LEGAL_FORMS = (
    "LLC, Ltd, Limited, Inc, Corp, Corporation, Co, SA, S.A., SA de CV, SAL, SARL, "
    "GmbH, AG, BV, NV, BVBA, SPA, SRL, PLC, Pte, FZE, FZCO, FZ LLC, DMCC, DOOEL, "
    "AD, OU, OOO, AO, OAO, PAO, ZAO, JSC, OJSC, PJSC, Kft, EOOD, SIA"
)

SCHEMA_DESC = (
    "- 'Person', if the name refers to an individual human.\n"
    "- 'Vessel', if the name refers to a ship. Not for the company that owns "
    "or manages it, even if the company name contains 'Offshore' or 'Shipping'.\n"
    "- 'Airplane', if the name refers to an aircraft.\n"
    f"- 'Company', for any entity whose name carries a legal form such as {LEGAL_FORMS}, "
    "or that is described as a company, firm, bank, exchange or business.\n"
    "- 'Organization', ONLY for unincorporated groups: terrorist groups, cartels, "
    "criminal organizations, hacker groups, political parties, government ministries "
    "and agencies, military and intelligence units.\n"
    "- 'LegalEntity', when it is unclear if the entity is a person, company or organization.\n"
    "NEVER invent new schema labels."
)
NAME_DESC = (
    "The name exactly as written in the article, with these removals: "
    "a parenthetical acronym or short reference following the name, e.g. "
    "'Ministry of Defense and Armed Forces Logistics (MODAFL)' becomes "
    "'Ministry of Defense and Armed Forces Logistics'; a quoted nickname inside a "
    "name, e.g. 'Nicolas \"Nicolasito\" Ernesto Maduro Guerra' becomes "
    "'Nicolas Ernesto Maduro Guerra'; a title, rank or honorific before the name. "
    "Keep legal-form suffixes such as LLC or S.A."
)
NATIONALITY_DESC = (
    "For a Person only: the nationality stated in the article, as a country name "
    "('Russia', not 'Russian'). Empty if not stated or if the entity is not a Person."
)
IMO_DESC = "For a Vessel only: the IMO number, only when explicitly stated."
FLAG_DESC = (
    "For a Vessel only: the country whose flag it flies, as a country name, e.g. "
    "'Panama-flagged' gives 'Panama'. Only when explicitly stated. The flag is "
    "not the vessel's country; leave country empty for a vessel."
)
# A good example of why we're restrictive about country is that it becomes less and less
# meaningful when you include every country someone or something operates or transacts in:
# Are we to include several countries in West Africa here too?
# "Mohammad Bazzi, who operates or transacts in or through Belgium, Lebanon, Iraq, and several
# countries in West Africa"
# We usually do find sources which can confirm the primary country of an entity, even if
# this article didn't for Mohammad Bazzi.
COUNTRY_DESC = (
    "The country where the entity is based: where a person resides, or where a "
    "company or organization is registered, incorporated or headquartered, as "
    "country names, e.g. 'Iran-based', 'Hong Kong-registered', 'a resident of Malta'. "
    "NOT countries where the entity merely operated, traded, shipped goods, held "
    "accounts, travelled or was seen: a Turkish company that ships to Iran is "
    "based in Turkey only. Do not infer a country from a nationality, a name, a "
    "flag, a language, or a city, region or province: 'Culiacan, Sinaloa-based' or "
    "'a Dubai office' does not give a country unless the article also names it."
)
RELATED_URL_DESC = (
    "URLs in the article whose target page is specifically about this entity, "
    "e.g. a State Department profile or rewards page for the person, an FBI wanted "
    "page, a Justice Department indictment announcement, or an OFAC enforcement "
    "action against the entity. Do NOT include links to OFAC 'Recent Actions' pages, "
    "sanctions program pages, executive orders, general licenses, FAQs or other "
    "pages that are not about this specific entity. Copy URLs exactly as written. "
    "Include links to further identifying information of a designee. Exclude links "
    "broadly about sanctions policy but not specifically about the named entity."
)


class Designee(BaseModel):
    entity_schema: Schema = Field(description=SCHEMA_DESC)
    name: str = Field(description=NAME_DESC)
    nationality: list[str] = Field(default_factory=list, description=NATIONALITY_DESC)
    imo: list[str] = Field(default_factory=list, description=IMO_DESC)
    flag: list[str] = Field(default_factory=list, description=FLAG_DESC)
    country: list[str] = Field(default_factory=list, description=COUNTRY_DESC)
    related_url: list[str] = Field(default_factory=list, description=RELATED_URL_DESC)


class Designees(BaseModel):
    designees: list[Designee]


PROMPT = f"""
<task>
Extract sanctions designees, linked entities, vessels and aircraft from this OFAC press release.
</task>

<scope>
The press release provides context for sanctions designations, so the scope is wide.
Include every NAMED entity that is:
- designated, sanctioned, identified as blocked property or otherwise the subject of
  this action, or
- designated or sanctioned previously and named in this press release, including
  designations recalled as background or as related recent actions, or
- linked to any of the above by a relationship the article states: owned or controlled
  by, acting for or on behalf of, a subsidiary, front, partner, counterparty, customer,
  supplier, employer, family member, associate, or the organization a person leads.
  Linked entities shouldn't include the organization an entity is a member of unless
  the press release is about the designation of that organization too.
  The organization lead by a person should be excluded if it's a government department,
  agency or unit unless it is itself designated.
Do NOT include entire governments (e.g. "Government of Iran") or vague nationwide
groups (e.g. "Assad regime") unless their designation is specifically mentioned.
Do NOT include entities that are only named for background and are not designated,
sanctioned or linked to a designee, and never include an entity the article does not
name (e.g. "an Iranian entity", "a Turkish company").
</scope>

<strict_requirements>
- NEVER infer, assume, or generate values not directly stated in the source text
- Extract ONLY information explicitly written in the article
- If data is not provided for a field, leave it empty
- Only extract entities the article names. NEVER construct a descriptive name for an
  unnamed entity, e.g. "Turkish company that imports motion control products"
- Do NOT extract aliases, nicknames, acronyms, online monikers, former names or any
  other alternative names. These are captured from OFAC's structured data elsewhere.
- Do not create or modify URLs
- Do not invent any country information
</strict_requirements>

<exclusions>
EXCLUDE from extraction:
- US Treasury officials (e.g., Secretary, Under Secretary)
- US federal government entities (e.g., Department of Treasury, SEC, OFAC itself)
- Other law enforcement officials or entities involved in the investigation, enforcement
  or prosecution of designated entities or their affiliates.
</exclusions>

<name_references>
This source introduces a subject with their full name followed by a short form in
brackets, and then uses the short form for the rest of the article, e.g.
"Retired Major General Denis Membreno Rivas (Membreno)". The bracketed short form,
the later references, and the rank are not part of the name. The name is
"Denis Membreno Rivas". Emit one entry per entity, not one per way of referring to it.
</name_references>

<extraction_fields>
For each entity found, extract these fields:

1. **name**: {NAME_DESC}

2. **entity_schema**: one of
{SCHEMA_DESC}

3. **nationality**: {NATIONALITY_DESC}

4. **imo**: {IMO_DESC}

5. **flag**: {FLAG_DESC}

6. **country**: {COUNTRY_DESC}

7. **related_url**: {RELATED_URL_DESC}
</extraction_fields>
"""


def crawl_item(
    context: Context,
    item: Designee,
    date: str,
    url: str,
    article_name: str,
    origin: str | None,
) -> None:
    entity = context.make(item.entity_schema)
    entity.id = context.make_id(item.name, *item.country)
    entity.add("name", item.name, origin=origin)
    nationality_prop = "nationality"
    if item.entity_schema != "Person":
        nationality_prop = "country"
    entity.add(nationality_prop, item.nationality, origin=origin)
    if entity.schema == "Vessel":
        entity.add("imoNumber", item.imo, origin=origin)
        entity.add("flag", item.flag, origin=origin)
    entity.add("country", item.country, origin=origin)
    entity.add("sourceUrl", item.related_url, origin=origin)
    entity.add("sourceUrl", url)

    article = h.make_article(context, url, title=article_name, published_at=date)
    documentation = h.make_documentation(context, entity, article)

    context.emit(entity)
    context.emit(article)
    context.emit(documentation)


def crawl_press_release(context: Context, url: str) -> None:
    article = context.fetch_html(url, cache_days=7, absolute_links=True)
    names = article.findall(".//h2[@class='uswds-page-title']")
    assert len(names) == 1, f"Expected 1 title, got {len(names)}"
    article_name = h.element_text(names[0])
    article_content = article.findall(".//article[@class='entity--type-node']")
    for img in article.findall(".//img"):
        # Images pasted from Office carry a megabytes-long base64 copy of the graphic here.
        if "o:gfxdata" in img.attrib:
            del img.attrib["o:gfxdata"]
        img_src = img.get("src")
        if img_src is None or img_src.startswith("data:image"):
            img_parent = img.getparent()
            if img_parent is not None:
                img_parent.remove(img)
    assert len(article_content) == 1
    article_element = article_content[0]
    date = h.xpath_strings(article_element, ".//time[@class='datetime']/@datetime")[0]
    article_html = tostring(article_element, pretty_print=True, encoding="unicode")
    assert all([article_name, article_html, date]), "One or more fields are empty"

    source_value = HtmlSourceValue(
        key_parts=url,
        label="Press Release",
        element=article_element,
        url=url,
    )
    prompt_result = run_typed_text_prompt(
        context, PROMPT, source_value.value_string, Designees
    )
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=prompt_result,
        origin=LLM_MODEL,
    )
    if not review.accepted:
        return

    for item in review.extracted_data.designees:
        crawl_item(context, item, date, url, article_name, review.origin)


def crawl(context: Context) -> None:
    page = 0
    while True:
        base_url = f"https://ofac.treasury.gov/press-releases?page={page}"
        doc = context.fetch_html(base_url, absolute_links=True)
        table = h.xpath_elements(doc, ".//table[contains(@class, 'views-table')]")
        next_page = h.xpath_elements(
            doc, ".//a[contains(@class, 'usa-pagination__next-page')]"
        )
        if not table or not next_page:
            break
        assert len(table) == 1, "Expected exactly one table in the document"
        for row in h.parse_html_table(table[0]):
            links = h.links_to_dict(row.pop("press_release_link"))
            url = next(iter(links.values()))
            if url is None:
                continue
            # Filter out unwanted download/media links
            if "/news/press-releases/" not in url:
                continue  # skip this row
            if "/index.php/" in url:
                url = url.replace("/index.php/", "/")
            crawl_press_release(context, url)
        page += 1
        context.flush()
        assert page < 200

    # FIXME: This is different from enforcement lists in that it's really just supporting
    # information; so it might be more OK to allow partial emit. Turning this on to create
    # something to enrich on. - FL Sep 3, 2025

    assert_all_accepted(context, raise_on_unaccepted=False)
