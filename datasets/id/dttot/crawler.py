from typing import Any, Dict
import xlrd  # type: ignore
from lxml import etree

from zavod import Context, Entity
from zavod import helpers as h
from zavod.shed.internal_data import fetch_internal_data


DATETIME_FORMAT = "%Y%m%d%H%M%S"


def parse_link(a: etree._Element) -> str:
    return a.get("href", "").split("/")[-1]


def row_to_dict(row, headers: Dict[str, str]) -> dict:
    out: Dict[str, Any] = {}
    for norm, col in headers.items():
        value = row[col].value
        if isinstance(value, str):
            value = value.strip()
        if value is None or value == "-" or value == "":
            continue
        out[norm] = value
    return out


def apply_date(entity: Entity, prop: str, value: str) -> None:
    """Dates come in arbitrary formats, but the most common ones seem to be
    01/04/1983 and 01 apr 1983. Sometimes there are multiple dates in the same cell.
    """
    if value is None or value == "00/00/0000":
        return None
    if isinstance(value, list):
        for v in value:
            apply_date(entity, prop, v)
    elif isinstance(value, (float, int)):
        entity.add(prop, h.convert_excel_date(value), original_value=str(value))
    elif isinstance(value, str):
        for value in h.multi_split(value, ["atau", "dan"]):
            h.apply_date(entity, prop, value)
    else:
        raise ValueError(f"Unexpected value type {type(value)}")


def crawl(context: Context):
    path = context.get_resource_path("source.xls")
    fetch_internal_data("id_dttot/20251208091237.xls", path)
    wb = xlrd.open_workbook(path)  # This will break loudly once they switch to XLSX
    sh = wb.sheet_by_index(0)
    in_header = sh.row(0)
    headers = {}
    for col_idx in range(sh.ncols):
        try:
            val = in_header[col_idx].value.strip().lower()
        except Exception:
            continue
        headers[context.lookup_value("headers", val, val)] = col_idx
    for rx in range(1, sh.nrows):
        drow = row_to_dict(sh.row(rx), headers)
        item_id = drow.pop("id")
        schema = context.lookup_value("type", drow.pop("type"), "LegalEntity")
        entity = context.make(schema)
        names = h.multi_split(drow.pop("name"), ["alias", "ALIAS"])
        entity.id = context.make_id(item_id, *names)
        entity.add("topics", "sanction")
        entity.add("name", names[0])
        entity.add("alias", names[1:])
        if addr := drow.pop("address", None):
            for addr in str(addr).split("\n"):
                addr = addr.strip().strip("-").strip()
                if addr == "N/A":
                    continue
                entity.add("address", addr, lang="ind")
        entity.add("country", drow.pop("country", None))
        entity.add("notes", drow.pop("description", None), lang="ind")
        if not entity.schema.is_a("Organization"):
            entity.add_cast("Person", "birthPlace", drow.pop("birth_place", None))
            dob_raw = drow.pop("birth_date", [])
            if dob_raw and dob_raw != "00/00/0000":
                entity.add_schema("Person")
                apply_date(entity, "birthDate", dob_raw)
        sanction = h.make_sanction(context, entity)
        sanction.add("authorityId", item_id)
        context.emit(entity)
        context.emit(sanction)
