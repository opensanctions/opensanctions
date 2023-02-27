import xlrd
from typing import List
from pathlib import Path
from normality import collapse_spaces
from urllib.parse import urljoin
from pantomime.types import XLS

from opensanctions.core import Context
from opensanctions import helpers as h


def parse_row(context: Context, headers: List[str], row: List[str]):
    entity = context.make("LegalEntity")
    entity.id = context.make_id(*row)
    sanction = h.make_sanction(context, entity)
    address = {}
    for (header, lang, type_), value in zip(headers, row):
        value = collapse_spaces(value)
        if value is None or value == "-":
            continue
        if header in ["index", "issuer"]:
            continue
        if type_ == "date":
            value = h.parse_date(value, ["%d/%m/%Y"])
        if header == "category":
            schema = context.lookup_value("categories", value)
            if schema is None:
                context.log.error("Unknown category", category=value)
            else:
                entity.add_schema(schema)
            continue
        if header in ("program", "listingDate", "endDate", "provisions"):
            sanction.add(header, value, lang=lang)
            continue
        if header in ["city", "country", "street"]:
            address[header] = value
            continue
        # print(header, value)
        entity.add(header, value, lang=lang)

    context.emit(sanction)
    context.emit(entity, target=True)
    if len(address):
        addr = h.make_address(context, **address)
        h.apply_address(context, entity, addr)


def parse_excel(context: Context, path: Path):
    xls = xlrd.open_workbook(path)
    for sheet in xls.sheets():
        headers = None
        for r in range(1, sheet.nrows):
            row = [h.convert_excel_cell(xls, c) for c in sheet.row(r)]
            if "#" in row[0]:
                headers = []
                for ara in row:
                    ara = collapse_spaces(ara)
                    result = context.lookup("headers", ara)
                    if result is None:
                        context.log.error("Unknown column", arabic=ara)
                        continue
                    headers.append((result.value, result.lang, result.type))
                # print(headers)
                continue
            if headers is None:
                continue
            parse_row(context, headers, row)


def crawl(context: Context):
    doc = context.fetch_html(context.source.data.url)
    found_file = False
    for a in doc.findall(".//a"):
        a_url = urljoin(context.source.data.url, a.get("href"))
        if "Local Terrorist List" in a.text_content() and "API/Upload" in a_url:
            found_file = True
            path = context.fetch_resource("source.xls", a_url)
            context.export_resource(path, XLS, title=context.SOURCE_TITLE)
            parse_excel(context, path)

    if not found_file:
        context.log.error("Could not download Local Terror excel file!")
