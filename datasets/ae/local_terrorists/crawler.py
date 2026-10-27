import re
from datetime import date, datetime, time

from typing import NamedTuple
from pathlib import Path
from normality import collapse_spaces, squash_spaces, stringify
from python_calamine import CalamineWorkbook
from rigour.mime.types import XLS

from zavod import Context
from zavod import helpers as h

PROGRAM_KEY = "AE-UNSC1373"


class HeaderSpec(NamedTuple):
    name: str
    lang: str


DATES = [
    re.compile(r"رفع الإدراج بموجب قرار مجلس الوزراء رقم \(\d{2}\) لسنة"),
    re.compile(r"مدرج بموجب قرار مجلس الوزراء رقم \(\d{2}\) لسنة"),
]


def parse_row(
    context: Context,
    headers: list[HeaderSpec],
    row: list[str | None],
    sanctioned: bool,
) -> None:
    entity_id = context.make_id(*row)
    schema = context.lookup_value("schema.override", entity_id, "LegalEntity")
    assert schema is not None
    entity = context.make(schema)
    entity.id = entity_id
    if sanctioned:
        entity.add("topics", "sanction")
    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
    address: dict[str, str] = {}
    identification: dict[str, str] = {}
    for header, value_ in zip(headers, row):
        value = squash_spaces(value_) if value_ is not None else None
        if value is None or value == "-":
            continue

        match header.name:
            case "index":
                continue
            case "category":
                schema = context.lookup_value("categories", value)
                if schema is None:
                    context.log.error(f'Unknown category "{value}"', category=value)
                elif not entity.schema.is_a("Vessel"):
                    entity.add_schema(schema)
            case "program" | "provisions":
                sanction.add(header.name, value, lang=header.lang)
            case "listingDate" | "endDate":
                for pattern in DATES:
                    value = re.sub(pattern, "", value)
                h.apply_date(sanction, header.name, value)
            case "city" | "country" | "street":
                address[header.name] = value
            case "birthDate":
                h.apply_date(entity, header.name, value)
            case (
                "issuer"
                | "document_number"
                | "document_type"
                | "doc_issue_date"
                | "doc_expiry_date"
            ):
                identification[header.name] = value
            case _:
                entity.add(header.name, value, lang=header.lang)

    if len(address):
        # Build address entity with detailed breakdown of components (street, city, country).
        # We use apply_address() instead of copy_address() because we want to emit
        # a separate Address entity with the full structured data, not just copy
        # the address string to the entity.
        addr = h.make_address(context, **address)
        h.apply_address(context, entity, addr)

    # h.make_identification creates an Entity only if the number is there
    if identification and identification.get("document_number"):
        # The lookup returns string "True" for passport types or None for non-passport documents.
        # We convert to boolean: any non-None/non-empty value becomes True.
        is_passport = bool(
            context.lookup_value(
                "is_passport",
                identification.get("document_type"),
                warn_unmatched=True,
            )
        )
        start_date = identification.get("doc_issue_date")
        end_date = identification.get("doc_expiry_date")

        for pattern in DATES:
            if start_date:
                start_date = re.sub(pattern, "", start_date)
            if end_date:
                end_date = re.sub(pattern, "", end_date)

        ident = h.make_identification(
            context,
            entity,
            number=identification.get("document_number"),
            country=identification.get("issuer"),
            start_date=start_date,
            end_date=end_date,
            passport=is_passport,
        )
        assert ident is not None, ident
        context.emit(ident)

    context.emit(sanction)
    context.emit(entity)


def parse_excel(context: Context, path: Path) -> None:
    workbook = CalamineWorkbook.from_path(path)
    for sheet_name in workbook.sheet_names:
        sheet = workbook.get_sheet_by_name(sheet_name)
        res = context.lookup("sanction_is_active", sheet_name)
        if res is None:
            context.log.warning("Unknown sheet", sheet=sheet_name)
            is_active = True
        else:
            is_active = res.is_active

        # Only the top-left cell of a merged range carries the value, so map
        # every cell of the range to that cell.
        merged_cells_map = {
            (r, c): (start_row, start_col)
            for (start_row, start_col), (end_row, end_col) in (
                sheet.merged_cell_ranges or []
            )
            for r in range(start_row, end_row + 1)
            for c in range(start_col, end_col + 1)
        }
        rows = sheet.to_python(skip_empty_area=False)
        headers: list[HeaderSpec] | None = None
        for r in range(1, len(rows)):
            row: list[str | None] = []
            for c in range(len(rows[r])):
                cell_r, cell_c = merged_cells_map.get((r, c), (r, c))
                value = rows[cell_r][cell_c]
                # Date cells keep a zero time part, e.g. "2023-07-26T00:00:00",
                # because whole rows feed into the entity IDs. Without it, the
                # entities would get new IDs.
                if type(value) is date:
                    value = datetime.combine(value, time())
                row.append(stringify(value))

            if row[0] is None:
                continue
            # "#" is the header for the first (index) column
            if "#" in row[0]:
                headers = []
                for header_text_ara in row:
                    # Finish when we hit the first empty cell in the header row
                    if header_text_ara is None:
                        break

                    header_text_ara = collapse_spaces(header_text_ara)
                    result = context.lookup("headers", header_text_ara)
                    if result is None:
                        context.log.error("Unknown column", arabic=header_text_ara)
                        continue
                    assert result.value is not None
                    headers.append(HeaderSpec(result.value, result.lang))
                # print(headers)
                continue
            if headers is None:
                continue
            parse_row(context, headers, row, is_active)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    xpath = ".//div[@class='local-list']//div[@class='file'][.//em[@class='icon-xlsx-file-extension-interface-symbol']]//a[contains(@href, 'DownloadFile')]"
    link = h.xpath_element(
        doc,
        xpath,
    )
    url = link.get("href")
    assert url is not None
    path = context.fetch_resource("source.xls", url)
    context.export_resource(path, XLS, title=context.SOURCE_TITLE)
    parse_excel(context, path)
