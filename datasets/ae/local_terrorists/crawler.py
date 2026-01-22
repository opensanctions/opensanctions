import xlrd
import re

from typing import List, Optional, NamedTuple
from pathlib import Path
from normality import collapse_spaces
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
    headers: List[HeaderSpec],
    row: List[Optional[str]],
    sanctioned: bool,
):
    entity_id = context.make_id(*row)
    schema = context.lookup_value("schema.override", entity_id, "LegalEntity")
    entity = context.make(schema)
    entity.id = entity_id
    if sanctioned:
        entity.add("topics", "sanction")
    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
    address = {}
    identification = {}
    for header, value_ in zip(headers, row):
        value = collapse_spaces(value_)
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


def parse_excel(context: Context, path: Path):
    # Pass formatting_info=True to get the merged cells
    xls = xlrd.open_workbook(path, formatting_info=True)
    for sheet in xls.sheets():
        res = context.lookup("sanction_is_active", sheet.name)
        if res is None:
            context.log.warning("Unknown sheet", sheet=sheet.name)
            is_active = True
        else:
            is_active = res.is_active

        # sheet.merged_cells is a list of tuples (rlo, rhi, clo, chi)
        # of row/column indices that are merged together.
        # cell (rlo, clo) (the top left one) will carry the data and
        # formatting info; the remainder will be recorded as blank cells
        # merged_cells_map is a map from any (r, c) in the merged cells
        # to the top left cell
        merged_cells_map = {
            (r, c): (rlo, clo)
            for (rlo, rhi, clo, chi) in sheet.merged_cells
            for r in range(rlo, rhi)
            for c in range(clo, chi)
        }
        headers: Optional[List[HeaderSpec]] = None
        for r in range(1, sheet.nrows):
            row = []
            for c in range(sheet.ncols):
                # Resolve the cell coordinates to the top left cell of the merged cell
                # or the cell itself if it's not part of a merged cell
                cell_coords = merged_cells_map.get((r, c)) or (r, c)
                cell = sheet.cell(*cell_coords)
                row.append(h.convert_excel_cell(xls, cell))

            # Skip empty rows (present because formatting_info=True)
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
                    headers.append(HeaderSpec(result.value, result.lang))
                # print(headers)
                continue
            if headers is None:
                continue
            parse_row(context, headers, row, is_active)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, absolute_links=True)
    section = h.xpath_element(doc, ".//h5[text()='Local Terrorist List']").getparent()
    assert section is not None, section
    link = h.xpath_element(
        section,
        ".//p[text()='Download Excel File']/ancestor::*[contains(@class,'download-file')]//a",
    )
    url = link.get("href")
    path = context.fetch_resource("source.xls", url)
    context.export_resource(path, XLS, title=context.SOURCE_TITLE)
    parse_excel(context, path)
