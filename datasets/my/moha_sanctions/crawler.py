from normality import squash_spaces
from rigour.mime.types import XML

from zavod import Context, helpers as h
from zavod.extract.zyte_api import fetch_html, fetch_resource
from zavod.util import Element

SOURCE_MEDIA_TYPE = "text/xml"
# The source is a Word document saved as a flat OPC package.
PKG = "{http://schemas.microsoft.com/office/2006/xmlPackage}"
W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
DOCUMENT_PART = "/word/document.xml"
# Tabs and breaks lay the text out in the narrow cells; they aren't separators.
WHITESPACE_TAGS = {f"{W}tab", f"{W}br", f"{W}cr"}
# Fields holding several values separate them with a pipe.
VALUE_SPLITS = ["|"]
# Names carry aliases after an at sign, e.g. "Mansor @ Termizi bin Mat Hussin".
NAME_SPLITS = ["@"]
# Identifier fields use a slash in addition to the pipe.
NUMBER_SPLITS = ["|", "/"]
# All entries sit in a single section, so individuals and groups are told apart
# by the set of columns each entry carries.
PERSON_COLUMNS = 13
ORGANIZATION_COLUMNS = 7


def clean_value(value: str) -> str | None:
    """Return None for the placeholder the source uses for missing values."""
    value = value.strip()
    return None if value in ("", "-") else value


def cell_text(cell: Element) -> str:
    """Join a cell's values with the pipe the fields split on.

    Each value is a numbered paragraph; an unnumbered one continues the value
    above it.
    """
    values: list[str] = []
    for paragraph in cell.findall(f"{W}p"):
        parts: list[str] = []
        for node in paragraph.iter():
            if node.tag == f"{W}t":
                parts.append(node.text or "")
            elif node.tag in WHITESPACE_TAGS:
                parts.append(" ")
        text = squash_spaces("".join(parts))
        if not text:
            continue
        if paragraph.find(f"{W}pPr/{W}numPr") is not None or not values:
            values.append(text)
        else:
            values[-1] = f"{values[-1]} {text}"
    return " | ".join(values)


def parse_entry(
    context: Context, header: list[str], cells: list[str]
) -> dict[str, str | None]:
    """Map an entry's cells onto the stable keys used below.

    Column labels are numbered and their wording varies between the individual
    and group tables (and contains typos), so they are resolved through a
    lookup rather than slugified.
    """
    row: dict[str, str | None] = {}
    for label, value in zip(header, cells, strict=True):
        key = context.lookup_value("columns", label)
        if key is None:
            raise ValueError(f"Unknown column: {label!r}")
        if key in row:
            raise ValueError(f"Duplicate column: {label!r}")
        row[key] = clean_value(value)
    return row


def crawl_entry(context: Context, row: dict[str, str | None]) -> None:
    if len(row) == PERSON_COLUMNS:
        assert "birth_date" in row, row
        schema, key = "Person", "person"
    elif len(row) == ORGANIZATION_COLUMNS:
        schema, key = "Organization", "group"
    else:
        raise ValueError(f"Unexpected number of columns: {len(row)}")

    reference = row.pop("reference_no")
    if reference is None:
        raise ValueError(f"Entry has no reference number: {row}")

    entity = context.make(schema)
    entity.id = context.make_slug(key, reference)
    entity.add("name", h.multi_split(row.pop("name"), NAME_SPLITS))
    entity.add("topics", "sanction")
    entity.add("address", h.multi_split(row.pop("address"), VALUE_SPLITS))
    for field in ("alias", "other_name"):
        entity.add("alias", h.multi_split(row.pop(field, None), VALUE_SPLITS))

    if entity.schema.is_a("Person"):
        entity.add("title", row.pop("title"))
        h.apply_date(entity, "birthDate", row.pop("birth_date"))
        entity.add("birthPlace", row.pop("birth_place"))
        entity.add("nationality", row.pop("citizenship"))
        entity.add("position", row.pop("position"))
        passport_no = row.pop("passport_no")
        entity.add("passportNumber", h.multi_split(passport_no, NUMBER_SPLITS))
        entity.add("idNumber", h.multi_split(row.pop("id_no"), NUMBER_SPLITS))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", row.pop("date_listed"))
    sanction.add("authorityId", reference)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row, ignore=["internal_no"])


def crawl_xml_url(context: Context) -> str:
    page_title_xpath = ".//*[contains(text(), 'LIST OF SANCTIONS UNDER THE MINISTRY OF HOME AFFAIRS (MOHA)')]"
    html = fetch_html(
        context, context.data_url, page_title_xpath, cache_days=5, absolute_links=True
    )
    xml_link_xpath = ".//div[@class='uk-container']//a[contains(., 'sanctions list') and contains(@href, '.xml')]/@href"
    return h.xpath_string(html, xml_link_xpath)


def crawl(context: Context) -> None:
    xml_url = crawl_xml_url(context)
    _, _, _, path = fetch_resource(
        context,
        "source.xml",
        xml_url,
        expected_media_type=SOURCE_MEDIA_TYPE,
        geolocation="MY",
    )
    context.export_resource(path, XML, title=context.SOURCE_TITLE)

    doc = context.parse_resource_xml(path)
    part = doc.getroot().find(f"{PKG}part[@{PKG}name='{DOCUMENT_PART}']")
    assert part is not None, f"The package has no {DOCUMENT_PART} part"
    entries = 0
    # Each table repeats the header row.
    for table in part.findall(f".//{W}tbl"):
        rows = table.findall(f"{W}tr")
        header = [cell_text(cell) for cell in rows[0].findall(f"{W}tc")]
        assert header[0] == "(1) No.", header
        for table_row in rows[1:]:
            cells = [cell_text(cell) for cell in table_row.findall(f"{W}tc")]
            crawl_entry(context, parse_entry(context, header, cells))
            entries += 1
    assert entries > 0, "No entries found in the source document"
