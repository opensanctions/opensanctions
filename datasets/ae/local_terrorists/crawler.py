import xlrd
import re

from typing import List, Optional
from pathlib import Path
from normality import collapse_spaces
from rigour.mime.types import XLS

from zavod import Context
from zavod import helpers as h


DATES = [
    re.compile(r"رفع الإدراج بموجب قرار مجلس الوزراء رقم \(\d{2}\) لسنة"),
    re.compile(r"مدرج بموجب قرار مجلس الوزراء رقم \(\d{2}\) لسنة"),
]


def parse_row(
    context: Context, headers: List[str], row: List[Optional[str]], sanctioned: bool
):
    entity_id = context.make_id(*row)
    schema = context.lookup_value("schema.override", entity_id, "LegalEntity")
    entity = context.make(schema)
    entity.id = entity_id
    if sanctioned:
        entity.add("topics", "sanction")
    sanction = h.make_sanction(context, entity)
    address = {}
    for (header, lang, type_), value_ in zip(headers, row):
        value = collapse_spaces(value_)
        if value is None or value == "-":
            continue
        if header in ["index", "issuer"]:
            continue
        if header == "category":
            schema = context.lookup_value("categories", value)
            if schema is None:
                context.log.error("Unknown category", category=value)
            elif not entity.schema.is_a("Vessel"):
                entity.add_schema(schema)
            continue
        if header in ("program", "provisions"):
            sanction.add(header, value, lang=lang)
            continue
        if header in ("listingDate", "endDate"):
            for pattern in DATES:
                value = re.sub(pattern, "", value)
            h.apply_date(sanction, header, value)
            continue
        if header in ["city", "country", "street"]:
            address[header] = value
            continue
        # print(header, value)
        if header in ["birthDate"]:
            h.apply_date(entity, header, value)
        else:
            entity.add(header, value, lang=lang)

    if len(address):
        addr = h.make_address(context, **address)
        h.copy_address(entity, addr)

    context.emit(sanction)
    context.emit(entity)


def parse_excel(context: Context, path: Path):
    xls = xlrd.open_workbook(path)
    for sheet in xls.sheets():
        sanctioned = "رفع الإدراج" not in sheet.name
        headers: Optional[List[str]] = None
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
            parse_row(context, headers, row, sanctioned)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    section = doc.xpath(".//h5[text()='Local Terrorist List']")[0].getparent()
    link = section.xpath(
        ".//p[text()='Download Excel File']/ancestor::*[contains(@class,'download-file')]//a"
    )[0]
    url = link.get("href")
    path = context.fetch_resource("source.xls", url)
    context.export_resource(path, XLS, title=context.SOURCE_TITLE)
    parse_excel(context, path)
