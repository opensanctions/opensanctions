from typing import Dict, List, Optional
import re

from pydantic import BaseModel, Field
from rigour.mime.types import PDF
from rigour.names.org_types import extract_org_types
from pdfplumber.page import Page

from zavod import Context, helpers as h
from zavod.shed.gpt import run_typed_text_prompt
from zavod.shed.zyte_api import fetch_html, fetch_resource
from zavod.stateful.review import get_review, request_review

# Cases
#
# last name, forenames or initials
# Baer, Gregory Sherwin
#
# last name, forenames or initials, name suffix
# Hereford, Sonnie, W., III
#
# Last name, forenames or initials, qualification/role
# !!! similar to name suffix example
# Caldwell, John Ed, Pharmacist
# Bishop, Lisa Renee, RN
#
# Company name, Legal form, City, State
# Dunn Medical, Inc., Eufaula, Alabama
# !!! similar to person names with roles


# Surname, First Name, Middle Name
# Maybe followed by an initial, and maybe a period.
# Watch out for accepting same suffix as part of simple names e.g. Jr. or III
SIMPLE_NAME_PATTERN = r"[A-Z][a-z]+, ([A-Z][a-z]+ )*[A-Z]([a-z]*|\.)"
SIMPLE_NAME_REGEX = re.compile(SIMPLE_NAME_PATTERN)
NAME_SUFFIX_PATTERN = r"[IVX]{2,3}|Jr\.?|Sr\.?"
NAME_SUFFIX_REGEX = re.compile(NAME_SUFFIX_PATTERN)
NAME_WITH_SUFFIX_REGEX = re.compile(
    rf"(?P<name>{SIMPLE_NAME_PATTERN}), (?P<suffix>{NAME_SUFFIX_PATTERN})$"
)
# Simple name, comma, and one word or an acronym
NAME_WITH_ROLE_REGEX = re.compile(
    rf"(?P<name>{SIMPLE_NAME_PATTERN}), (?P<role>(([A-Z][a-z]+ ?)+|[A-Z]{{2,4}}))"
)
MODEL_VERSION = 1
MIN_MODEL_VERSION = 1

sector_field = Field(
    default=None,
    description=(
        "The sector, qualification or professional title of the entity if included "
        "along with their name. Often an acronym. Don't fill this based on my prompt, "
        "only from the text."
    ),
)


class Entity(BaseModel):
    name: str
    name_suffix: Optional[str] = None
    aliases: Optional[str] = None
    sector: Optional[str] = sector_field


class RelatedEntity(Entity):
    relationship_role: Optional[str] = Field(
        description=(
            "Use `owner` if they are listed as the owner, otherwise use the "
            "relationship role precisely as listed, e.g. `Vice President`."
        )
    )


class RootEntity(Entity):
    address: Optional[str] = None
    related_entities: List[RelatedEntity] = Field(
        description=(
            "Owners or other officers of a company if listed. If the "
            "first entity looks like a person and a single owner name is included in"
            " parentheses, then only give one entity - the person. Don't make a company"
            " out of the person's name."
        ),
        default=[],
    )


PROMPT = """
Extract the entities from the attached text. The text is details of debarred medicaid
 providers.

Leave anything blank that is not present in the text.
"""


def crawl_row(context, names, category, start_date):
    entity_data = None
    if SIMPLE_NAME_REGEX.fullmatch(names):
        if extract_org_types(names):
            context.log.debug(
                "Name looks like a company name, not as simple as persons", names=names
            )
        elif NAME_SUFFIX_REGEX.search(names):
            context.log.debug("name contains suffix", names=names)
        else:
            entity_data = RootEntity(name=names)

    if entity_data is None:
        if match := NAME_WITH_SUFFIX_REGEX.fullmatch(names):
            context.log.debug(
                "name with suffix",
                name=match.group("name"),
                suffix=match.group("suffix"),
            )
            entity_data = RootEntity(
                name=match.group("name"),
                name_suffix=match.group("suffix"),
            )

    if entity_data is None:
        if match := NAME_WITH_ROLE_REGEX.fullmatch(names):
            context.log.debug(
                "name with role",
                name=match.group("name"),
                role=match.group("role"),
            )
            entity_data = RootEntity(
                name=match.group("name"),
                sector=match.group("role"),
            )

    if entity_data is None:
        context.log.debug("unsure about", names=names)
        review = get_review(context, RootEntity, names, MIN_MODEL_VERSION)
        if review is None:
            prompt_result = run_typed_text_prompt(
                context, PROMPT, names, response_type=RootEntity
            )
            review = request_review(
                context,
                key_parts=names,
                source_value=names,
                source_mime_type="text/plain",
                source_label="Debarred entities",
                source_url=None,
                orig_extraction_data=prompt_result,
                model_version=MODEL_VERSION,
            )
        if not review.accepted:
            return
        entity_data = review.extracted_data

    entity = context.make("LegalEntity")
    entity.id = context.make_id(entity_data.name, entity_data.sector)
    entity.add("name", entity_data.name)
    entity.add("alias", entity_data.aliases)
    entity.add("country", "us")
    entity.add("topics", "debarment")
    if entity_data.sector and "imposter" in entity_data.sector.lower():
        entity.add("description", entity_data.sector)
    else:
        entity.add("sector", entity_data.sector)
    entity.add("sector", category)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", start_date)

    context.emit(entity)
    context.emit(sanction)

    for item in entity_data.related_entities:
        related = context.make("LegalEntity")
        related.id = context.make_id(item.name, item.sector)
        related.add("name", item.name)
        related.add("country", "us")
        related.add("topics", "debarment")
        if item.sector and "imposter" in item.sector.lower():
            related.add("description", item.sector)
        else:
            related.add("sector", item.sector)

        if item.role == "owner":
            schema = "Ownership"
            from_prop = "owner"
            to_prop = "asset"
        else:
            schema = "UnknownLink"
            from_prop = "subject"
            to_prop = "object"
        relation = context.make(schema)
        relation.id = context.make_id(entity.id, related.id)
        relation.add(from_prop, entity)
        relation.add(to_prop, related)
        relation.add("role", item.role)

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "startDate", start_date)
        context.emit(related)
        context.emit(relation)
        context.emit(sanction)


def crawl_data_url(context: Context):
    file_xpath = "//a[contains(., 'PDF Version')]"
    doc = fetch_html(context, context.data_url, file_xpath)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(file_xpath)[0].get("href")


def page_settings(page: Page) -> Dict:
    settings = {"join_y_tolerance": 15}
    if page.page_number == 1:
        # The table header is a little box above the main table, so it gets detected as a separate table.
        tables = page.find_tables()
        table_start_y = tables[0].bbox[1]
        # im = page.to_image()
        # im.draw_hline(table_start)
        # im.save("page.png")
        page = page.crop((0, table_start_y, page.width - 15, page.height - 15))
    return page, settings


def crawl(context: Context) -> None:
    # The .xls file first seemed to work, then a newer file couldn't be parsed
    # as a valid Compond Document file.
    # xlrd gave "xlrd.compdoc.CompDocError: MSAT extension: accessing sector ..."
    # https://stackoverflow.com/questions/74262026/reading-the-excel-file-from-python-pandas-given-msat-extension-error
    # didn't work.

    # First we find the link to the PDF file
    url = crawl_data_url(context)
    _, _, _, path = fetch_resource(context, "source.pdf", url, expected_media_type=PDF)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    try:
        category = None
        for row in h.parse_pdf_table(context, path, page_settings=page_settings):
            name = row.pop("name_of_provider").replace("\n", " ").strip()
            if name == "":
                continue
            start_date = row.pop("suspension_effective_date").strip()
            # When there's no date, we're probably at a category header row.
            if start_date == "":
                if context.lookup("categories", name):
                    category = name
                else:
                    category = None
                    context.log.warning(
                        "Unexpected category. Confirm we're parsing the PDF correctly.",
                        category=name,
                    )
                continue
            crawl_row(context, name, category, start_date)
    except Exception as e:
        if "No table found on page 49" in str(e):
            # this is where the right-hand side of the table starts wrapping
            pass
        else:
            if "No table found on page" in str(e):
                raise RuntimeError(
                    "PDF pages changed. See if they've upgraded to xlsx or update max page."
                )
            else:
                raise
