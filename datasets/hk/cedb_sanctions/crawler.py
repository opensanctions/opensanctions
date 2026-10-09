from lxml.etree import _Element as Element
from normality import squash_spaces
from rigour.mime.types import XML
from zavod.shed.un_sc import get_legal_entities, get_persons, load_un_sc

from zavod import Context, Entity
from zavod import helpers as h


COMMON_TAGS = {
    "DATAID",
    "VERSIONNUM",
    "FIRST_NAME",
    "UN_LIST_TYPE",
    "REFERENCE_NUMBER",
    "LISTED_ON",
    "COMMENTS1",
    "NAME_ORIGINAL_SCRIPT",
    "LIST_TYPE",
    "LAST_DAY_UPDATED",
    "LAST_REVIEWED_ON",
    "SORT_KEY",
    "SORT_KEY_LAST_MOD",
    "HAS_INTERPOL_LINK",
    "INTERPOL_LINK",
}
INDIVIDUAL_TAGS = COMMON_TAGS | {
    "SECOND_NAME",
    "THIRD_NAME",
    "FOURTH_NAME",
    "GENDER",
    "TITLE",
    "DESIGNATION",
    "NATIONALITY",
    "INDIVIDUAL_ALIAS",
    "INDIVIDUAL_ADDRESS",
    "INDIVIDUAL_DATE_OF_BIRTH",
    "INDIVIDUAL_PLACE_OF_BIRTH",
    "INDIVIDUAL_DOCUMENT",
    "DESIG",
    "ADDRESS",
    "GOODQUALITY",
    "PASSPORT",
}
ENTITY_TAGS = COMMON_TAGS | {"ENTITY_ALIAS", "ENTITY_ADDRESS"}


def text(node: Element, path: str) -> str | None:
    value = node.findtext(path)
    if value is None:
        return None
    return squash_spaces(value) or None


def audit_tags(context: Context, node: Element, known: set[str]) -> None:
    unknown = {str(child.tag) for child in node} - known
    if unknown:
        context.log.warning(
            "Unknown elements in record",
            parent=node.tag,
            tags=sorted(unknown),
            ref=text(node, "./REFERENCE_NUMBER"),
        )


def parse_individual(context: Context, node: Element, person: Entity) -> None:
    audit_tags(context, node, INDIVIDUAL_TAGS)
    person.add("idNumber", text(node, "./REFERENCE_NUMBER"))
    context.emit(person)


def parse_entity(context: Context, node: Element, entity: Entity) -> None:
    audit_tags(context, node, ENTITY_TAGS)
    entity.add("idNumber", text(node, "./REFERENCE_NUMBER"))
    context.emit(entity)


def crawl_file(context: Context, name: str, url: str, title: str) -> None:
    path = context.fetch_resource(f"{name}.xml", url)
    context.export_resource(path, XML, title=f"Source data - {title}")
    doc = context.parse_resource_xml(path)
    prefix = context.dataset.prefix
    assert prefix is not None, "Dataset prefix is required"
    for node, person in get_persons(context, prefix, doc):
        parse_individual(context, node, person)
    for node, entity in get_legal_entities(context, prefix, doc):
        parse_entity(context, node, entity)


def crawl(context: Context) -> None:
    _, un_doc = load_un_sc(context)

    # The dataset page lists one XML resource per regime, each linked twice
    # (details and download).
    page = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    files: dict[str, str] = {}
    for link in h.xpath_elements(page, "//a[contains(@href, '/datagovhk/TFSlists/')]"):
        url = link.get("href")
        if url is None or not url.endswith(".xml"):
            continue
        name = url.rsplit("/", 1)[-1].removesuffix(".xml")
        files[name] = url
    if len(files) == 0:
        raise ValueError(f"No XML resources found on {context.data_url}")

    # Compare against the files we expect, so that a regime being added to or
    # dropped from the page is noticed. Unexpected files are still crawled.
    expected: list[str] = context.dataset.config["expected_files"]
    missing = set(expected) - set(files)
    if missing:
        context.log.warning("Expected files missing from page", files=sorted(missing))
    unexpected = set(files) - set(expected)
    if unexpected:
        context.log.warning("Unexpected files on page", files=sorted(unexpected))

    for name, url in sorted(files.items()):
        title = name.replace("_", " ").title()
        crawl_file(context, name, url, title)
