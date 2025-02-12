import shutil
from typing import Optional, List
from normality import slugify, stringify
from openpyxl import load_workbook
from datetime import datetime
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h


def parse_countries(countries: Optional[str]) -> List[str]:
    parsed: List[str] = []
    if countries is None:
        return parsed
    for country in countries.split(", "):
        country = country.strip()
        if len(country):
            parsed.append(country)
    return parsed


def header_names(cells):
    headers = []
    for idx, cell in enumerate(cells):
        if cell is None:
            cell = f"column_{idx}"
        headers.append(slugify(cell, "_"))
    return headers


def excel_records(path):
    wb = load_workbook(filename=path, read_only=True)
    for sheet in wb.worksheets:
        headers = None
        for idx, row in enumerate(sheet.rows):
            cells = [c.value for c in row]
            if headers is None:
                headers = header_names(cells)
                continue
            record = {}
            for header, value in zip(headers, cells):
                if isinstance(value, datetime):
                    value = value.date()
                value = stringify(value)
                if value is not None:
                    record[header] = value
            yield record


def crawl(context: Context):
    assert context.dataset.base_path is not None
    data_path = context.dataset.base_path / "data.xlsx"
    path = context.get_resource_path("source.xlsx")
    shutil.copyfile(data_path, path)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    for row in excel_records(path):
        entity_type = row.pop("entity", None)
        if entity_type is None:
            continue
        schema = context.lookup_value("types", entity_type)
        if schema is None:
            context.log.warning("Unknown entity type", entity=entity_type)
            continue
        entity = context.make(schema)
        title = row.pop("title")
        entity.id = context.make_slug(entity_type, title)
        entity.add("name", title)
        entity.add("alias", row.pop("other_name", None))
        entity.add("country", parse_countries(row.pop("country", None)))
        for country in parse_countries(row.pop("nationality", None)):
            prop = "nationality" if schema == "Person" else "jurisdiction"
            entity.add(prop, country)

        sanction = h.make_sanction(context, entity)
        # sanction.add("status", row.pop("statusName"))
        sanction.add("reason", row.pop("grounds", None))
        sanction.add("authority", row.pop("source", None))
        sanction.add("authority", row.pop("idb_sanction_source", None))
        sanction.add("program", row.pop("idb_sanction_type", None))
        h.apply_date(sanction, "startDate", row.pop("from", None))
        # Sometimes row.to is "Ongoing", which will be datapatched to null end_date
        h.apply_date(sanction, "endDate", row.pop("to", None))

        is_debarred = h.is_active(sanction)
        if is_debarred:
            entity.add("topics", "debarment")

        context.emit(sanction)
        context.emit(entity)
        context.audit_data(row)
