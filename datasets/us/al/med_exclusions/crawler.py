import re
from typing import Any, List

from normality import squash_spaces
from pdfplumber.page import Page
from pydantic import BaseModel, Field
from rigour.mime.types import PDF
from rigour.names.org_types import extract_org_types
from zavod.extract.llm import DEFAULT_MODEL, run_typed_text_prompt
from zavod.extract import zyte_api
from zavod.stateful.review import (
    TextSourceValue,
    assert_all_accepted,
    review_extraction,
)

from zavod import Context, entity
from zavod import helpers as h

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
CRAWLER_VERSION = 2

POSITIONS_DESCRIPTION = (
    "The positions held by the person for an entity who is a person. "
    "Populate this precisely as listed in the text when the text indicates "
    "the job role of a person, otherwise leave empty. Sometimes this is an "
    "abbreviation of a job title, e.g. RN for Registered Nurse."
)


class Entity(BaseModel):
    name: str
    name_suffix: str | None = None
    aliases: List[str] = []
    positions: List[str] = Field(default=[], description=POSITIONS_DESCRIPTION)
    address: str | None = None


RELATIONSHIP_DESCRIPTION = (
    "Relationship between the entities e.g. `Owner` or `Vice President`."
)


class RelatedEntity(Entity):
    relationship_role: str | None = Field(description=RELATIONSHIP_DESCRIPTION)


RELATED_ENTITIES_DESCRIPTION = (
    "Owners or other officers of a company if listed. If the "
    "first entity looks like a person and a single owner name is included in"
    " parentheses, then only give one entity - the person. Don't make a company"
    " out of the person's name."
)


class RootEntity(Entity):
    related_entities: List[RelatedEntity] = Field(
        default=[], description=RELATED_ENTITIES_DESCRIPTION
    )


PROMPT = f"""
Extract the entities from the attached text. The text is details of debarred medicaid
 providers.

Leave fields blank if information is not present in the source material.
NEVER infer, assume, or generate values that are not directly stated.

NEVER rearrange names from last name, first name order to full names.

Names may contain a suffix, e.g. Jr. or III. They may also include a role, e.g.
RN or Registered Nurse. Leave the suffix as part of the name but move the role
to the positions field.

Specific fields:

`related_entities`: {RELATED_ENTITIES_DESCRIPTION}

`positions`: {POSITIONS_DESCRIPTION}

`relationship_role`: {RELATIONSHIP_DESCRIPTION}

`name_suffix`: This field MUST be null.
"""


def apply_comma_name(entity: entity.Entity, name: str) -> None:
    parts = name.split(",")
    if (
        len(parts) == 2
        and not extract_org_types(name)
        and not NAME_SUFFIX_REGEX.search(name)
    ):
        entity.add_schema("Person")
        forenames = parts[1].split()
        h.apply_name(
            entity,
            first_name=forenames[0],
            middle_name=forenames[1] if len(forenames) > 1 else None,
            name3=forenames[2] if len(forenames) > 2 else None,
            name4=forenames[3] if len(forenames) > 3 else None,
            name5=forenames[4] if len(forenames) > 4 else None,
            last_name=parts[0],
        )
    else:
        entity.add("name", name)


def crawl_row(
    context: Context, names: str, category: str | None, start_date: str, filename: str
) -> None:
    origin = None
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
            origin = filename

    if entity_data is None:
        # It's correct to include the suffix in the name, but we want to identify
        # the case to distinguish it from a role after a comma.
        if match := NAME_WITH_SUFFIX_REGEX.fullmatch(names):
            context.log.debug(
                "name with suffix",
                name=match.group("name"),
                suffix=match.group("suffix"),
            )
            entity_data = RootEntity(name=names, name_suffix=match.group("suffix"))
            origin = filename

    if entity_data is None:
        if match := NAME_WITH_ROLE_REGEX.fullmatch(names):
            context.log.debug(
                "name with role", name=match.group("name"), role=match.group("role")
            )
            entity_data = RootEntity(
                name=match.group("name"),
                positions=[match.group("role")],
            )
            origin = filename

    if entity_data is None:
        context.log.debug("unsure about names", names=names)
        source_value = TextSourceValue(
            key_parts=names, label='"Name of provider" column', text=names
        )
        prompt_result = run_typed_text_prompt(
            context, PROMPT, source_value.value_string, response_type=RootEntity
        )
        review = review_extraction(
            context,
            crawler_version=CRAWLER_VERSION,
            source_value=source_value,
            original_extraction=prompt_result,
            origin=DEFAULT_MODEL,
        )
        if not review.accepted:
            return
        entity_data = review.extracted_data
        origin = review.origin

    entity = context.make("LegalEntity")
    # Passing *postitions would re-key, so we keep it for now and ignore the type error
    entity.id = context.make_id(entity_data.name, entity_data.positions)  # type: ignore[arg-type]
    apply_comma_name(entity, entity_data.name)
    entity.add_cast("Person", "nameSuffix", entity_data.name_suffix)
    entity.add("alias", entity_data.aliases, origin=origin)
    entity.add("address", entity_data.address, origin=origin)
    entity.add("country", "us")
    entity.add("topics", "debarment")
    if any("imposter" in p.lower() for p in entity_data.positions):
        entity.add("description", entity_data.positions, origin=origin)
    else:
        entity.add_cast("Person", "position", entity_data.positions)
    entity.add("sector", category, origin=filename)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", start_date)

    context.emit(entity)
    context.emit(sanction)

    for item in entity_data.related_entities:
        related = context.make("LegalEntity")
        # Passing *postitions would re-key, so we keep it for now and ignore the type error
        related_id = context.make_id(item.name, item.positions)  # type: ignore[arg-type]
        if related_id == entity.id:
            continue
        related.id = related_id

        apply_comma_name(related, item.name)
        related.add_cast("Person", "nameSuffix", item.name_suffix)
        related.add("alias", item.aliases, origin=origin)
        related.add("address", item.address, origin=origin)
        related.add("country", "us")
        related.add("topics", "debarment")
        if any("imposter" in p.lower() for p in item.positions):
            related.add("description", item.positions, origin=origin)
        else:
            related.add_cast("Person", "position", item.positions)
        related.add("sector", category, origin=filename)

        # Extracting directionality is tricky because sometimes the asset is
        # the first entity, sometimes it's in parentheses.
        relation = context.make("UnknownLink")
        relation.id = context.make_id(entity.id, related.id)
        relation.add("subject", entity)
        relation.add("object", related)
        relation.add("role", item.relationship_role, origin=origin)

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "startDate", start_date)
        context.emit(related)
        context.emit(relation)
        context.emit(sanction)


def crawl_data_url(context: Context) -> str:
    file_xpath = "//a[contains(., 'PDF Version')]"
    doc = zyte_api.fetch_html(
        context, context.data_url, unblock_validator=file_xpath, absolute_links=True
    )
    url = h.xpath_string(doc, file_xpath + "/@href")
    assert url is not None, "Could not find PDF URL"
    return url


def page_settings(page: Page) -> tuple[Page, dict[str, Any]]:
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
    _, _, _, path = zyte_api.fetch_resource(
        context, "source.pdf", url, expected_media_type=PDF
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    filename = url.split("/")[-1]
    assert ".pdf" in filename, filename

    try:
        category = None
        for row in h.parse_pdf_table(context, path, page_settings=page_settings):
            name_raw = row.pop("name_of_provider")
            assert name_raw is not None
            name = squash_spaces(name_raw)
            if name == "":
                continue
            start_date_raw = row.pop("suspension_effective_date")
            assert start_date_raw is not None
            start_date = start_date_raw.strip()
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
            crawl_row(context, name, category, start_date, filename)
        assert_all_accepted(context)
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
