from rigour.mime.types import PDF
import re
from normality import squash_spaces
from urllib.parse import urljoin

from zavod import Context
from zavod.entity import Entity
from zavod import helpers as h
from zavod.stateful.review import assert_all_accepted
from zavod.util import Element
from zavod.extract.zyte_api import fetch_html, fetch_resource


ASSOCIATIONS_LABEL = "UNLAWFUL ASSOCIATIONS UNDER SECTION 3 OF UNLAWFUL ACTIVITIES (PREVENTION) ACT, 1967"
ORGANISATIONS_LABEL = "TERRORIST ORGANISATIONS LISTED IN THE FIRST SCHEDULE OF THE UNLAWFUL ACTIVITIES (PREVENTION) ACT, 1967"
INDIVIDUALS_LABEL = "INDIVIDUALS TERRORISTS LISTED IN THE FOURTH SCHEDULE OF THE UNLAWFUL ACTIVITIES (PREVENTION) ACT, 1967"
RECORD_TITLE_XPATH = ".//div[@class='documentRecordTitle']"

# Requiring a capital letter at both ends keeps out bracketed words such as
# "(Khaplang)" or "(Maoist)", but admits mixed-case acronyms such as "(JeI)".
REGEX_ACRONYM_PARENS = re.compile(r"\s*\((?P<acronym>[A-Z][A-Za-z-]*[A-Z])\)")


def apply_names(context: Context, entity: Entity, names_string: str) -> None:
    """Apply the reviewed names, with a split of the source string as the suggestion."""
    if entity.schema.is_a("Person"):
        names = h.multi_split(names_string.rstrip("."), "@")
        suggested = h.Names(name=names[0], alias=names[1:])
    else:
        suggested = h.Names(
            name=REGEX_ACRONYM_PARENS.sub("", names_string).strip(),
            abbreviation=REGEX_ACRONYM_PARENS.findall(names_string),
        )
    is_irregular, suggested = h.check_names_regularity(entity, suggested)
    h.apply_reviewed_names(
        context,
        entity,
        original=h.Names(name=names_string),
        suggested=suggested,
        is_irregular=is_irregular,
    )


def crawl_entity(
    context: Context,
    *,
    schema: str,
    names_string: str,
    id_parts: tuple[str, ...],
    program: str,
    authority_id: str | None,
    source_url: str,
    detail_url: str | None,
) -> Entity:
    entity = context.make(schema)
    # Include aliases in ID because there are different individuals whose alias
    # is all that distinguishes them. The source adds and drops a trailing full
    # stop between list versions.
    entity.id = context.make_id(*id_parts, names_string.rstrip("."))
    apply_names(context, entity, names_string)
    entity.add("sourceUrl", source_url)
    entity.add("sourceUrl", detail_url)
    entity.add("topics", "sanction")

    sanction = h.make_sanction(
        context,
        entity,
        key=program,
        program_name=program,
        source_program_key=program,
        program_key=h.lookup_sanction_program_key(context, program),
    )
    sanction.add("authorityId", authority_id)

    context.emit(entity)
    context.emit(sanction)

    return entity


def crawl_row(
    context: Context,
    *,
    schema: str,
    row: str,
    program: str,
    authority_id: str | None,
    source_url: str,
    detail_url: str | None,
) -> None:
    """Emit the entities listed in one row, which usually lists a single entity."""

    def emit_listed(names_string: str, id_parts: tuple[str, ...] = ()) -> Entity:
        return crawl_entity(
            context,
            schema=schema,
            names_string=names_string,
            id_parts=id_parts,
            program=program,
            authority_id=authority_id,
            source_url=source_url,
            detail_url=detail_url,
        )

    split = context.lookup("split_rows", row)
    if split is None:
        if any(marker in row.lower() for marker in ("namely", "factions", " wing")):
            context.log.warning("Row may list several entities", row=row)
        emit_listed(row)
        return

    for listed in split.entities:
        entity = emit_listed(listed["raw_name"])
        if "related" in listed:
            assert entity.id is not None
            # A wing's name is only unique next to its parent: PREPAK and KCP
            # both have an armed wing called "Red Army".
            related = emit_listed(listed["related"]["raw_name"], (entity.id,))
            rel = context.make("UnknownLink")
            rel.id = context.make_id(entity.id, related.id)
            rel.add("subject", entity.id)
            rel.add("object", related.id)
            rel.add("role", listed["related"]["relationship"])
            context.emit(rel)


def collect_records(context: Context, url: str) -> list[tuple[str, str]]:
    """Collect (title, document URL) pairs from all pages of a document listing."""
    records: list[tuple[str, str]] = []
    page_url: str | None = url
    while page_url is not None:
        doc = fetch_html(
            context,
            page_url,
            RECORD_TITLE_XPATH,
            html_source="httpResponseBody",
            cache_days=1,
            absolute_links=True,
        )
        for title_el in h.xpath_elements(doc, RECORD_TITLE_XPATH):
            title = h.element_text(title_el)
            # Title and document link sit in sibling columns of the record row
            row = h.xpath_element(
                title_el, "./ancestor::div[contains(@class, 'row')][1]"
            )
            hrefs = h.xpath_strings(row, ".//a/@href", expect_exactly=1)
            records.append((title, hrefs[0]))
        next_hrefs = h.xpath_strings(doc, ".//a[@rel='next']/@href")
        page_url = urljoin(page_url, next_hrefs[0]) if next_hrefs else None
    return records


def crawl_associations(context: Context, url: str, program: str) -> None:
    records = collect_records(context, url)
    if len(records) == 0:
        context.log.error("No unlawful associations found", url=url)
    for names, detail_url in records:
        crawl_row(
            context,
            schema="Organization",
            row=names,
            program=program,
            authority_id=None,
            source_url=url,
            detail_url=detail_url,
        )


def crawl_organisations(context: Context, url: str, program: str) -> None:
    _, _, _, path = fetch_resource(
        context, "organisations.pdf", url, expected_media_type=PDF
    )
    context.export_resource(path, PDF, "organisations.pdf")
    for row in h.parse_pdf_table(context, path):
        authority_id = row.pop("s_no")
        names = row.pop("name_of_terrorist_organization")
        assert authority_id is not None and names is not None, row
        context.audit_data(row)
        crawl_row(
            context,
            schema="Organization",
            row=squash_spaces(names),
            program=program,
            authority_id=authority_id.rstrip("."),
            source_url=url,
            detail_url=None,
        )


def crawl_individuals(context: Context, url: str, program: str) -> None:
    records = collect_records(context, url)
    if len(records) == 0:
        context.log.error("No individuals found", url=url)
    for title, detail_url in records:
        crawl_row(
            context,
            schema="Person",
            row=title,
            program=program,
            authority_id=None,
            source_url=url,
            detail_url=detail_url,
        )


def get_link_by_label(doc: Element, label: str) -> str:
    label_xpath = f"{RECORD_TITLE_XPATH}[contains(text(), '{label}')]"
    title_el = h.xpath_element(doc, label_xpath)
    row = h.xpath_element(title_el, "./ancestor::div[contains(@class, 'row')][1]")
    return h.xpath_strings(row, ".//a/@href", expect_exactly=1)[0]


def crawl(context: Context) -> None:
    # the runner cannot resolve the site (NameResolutionError), fetch through Zyte
    doc = fetch_html(
        context,
        context.data_url,
        f"{RECORD_TITLE_XPATH}[contains(text(), '{ASSOCIATIONS_LABEL}')]",
        html_source="httpResponseBody",
        cache_days=1,
        absolute_links=True,
    )

    url = get_link_by_label(doc, ASSOCIATIONS_LABEL)
    crawl_associations(context, url, ASSOCIATIONS_LABEL)

    url = get_link_by_label(doc, ORGANISATIONS_LABEL)
    crawl_organisations(context, url, ORGANISATIONS_LABEL)

    url = get_link_by_label(doc, INDIVIDUALS_LABEL)
    crawl_individuals(context, url, INDIVIDUALS_LABEL)

    assert_all_accepted(context, raise_on_unaccepted=False)
