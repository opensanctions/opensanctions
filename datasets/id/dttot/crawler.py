import re
import xlrd  # type: ignore
from lxml import etree
from datetime import datetime

from zavod import Context, Entity
from zavod import helpers as h


DATETIME_FORMAT = "%Y%m%d%H%M%S"
addr_delim = re.compile(r"[\W][a-zA-Z]\)|;")


def is_sanctions_xlsx(a: etree._Element) -> bool:
    if (
        a.text is not None
        and a.text.strip().lower().find("dttot") > -1
        and (parse_link(a).endswith(".xls") or parse_link(a).endswith(".xlsx"))
    ):
        return True
    return False


def parse_link(a: etree._Element) -> str:
    return a.get("href", "").split("/")[-1]


def find_last_link(context: Context, html: etree._Element) -> str:
    last_time = datetime.strptime("19700101000000", DATETIME_FORMAT)
    last_link = None
    for link in html.findall(".//a"):
        if not is_sanctions_xlsx(link):
            continue
        try:
            ts_ = parse_link(link).split(".")[0]
            ts = datetime.strptime(ts_, DATETIME_FORMAT)
        except Exception:
            context.log.error(f"Could not parse timestamp from {link.get('href')}")
            continue
        if ts > last_time:
            last_time = ts
            last_link = link
    if last_link is None:
        raise ValueError("No link found")
    last_href = last_link.get("href")
    if last_href is None:
        raise ValueError("No href found")
    return last_href


def value_or_none(row, headers, key):
    try:
        return row[headers[key]].value
    except Exception:
        return None


def row_to_dict(row, headers: dict) -> dict:
    return {k: row[headers[k]].value for k in headers if value_or_none(row, headers, k)}


def apply_dob(entity: Entity, value: str) -> None:
    """Dates come in arbitrary formats, but the most common ones seem to be
    01/04/1983 and 01 apr 1983. Sometimes there are multiple dates in the same cell.
    """
    if value is None or value == "00/00/0000":
        return None
    if isinstance(value, list):
        for v in value:
            apply_dob(entity, v)
    elif isinstance(value, (float, int)):
        entity.add("birthDate", h.convert_excel_date(value), original_value=str(value))
    elif isinstance(value, str):
        for value in h.multi_split(value, ["atau"]):
            h.apply_date(entity, "birthDate", value)
    else:
        raise ValueError(f"Unexpected value type {type(value)}")


def crawl(context: Context):
    html = context.fetch_html(context.data_url)
    last_link = find_last_link(context, html)
    xls = context.fetch_resource("source.xls", last_link)
    wb = xlrd.open_workbook(xls)  # This will break loudly once they switch to XLSX
    sh = wb.sheet_by_index(0)
    in_header = sh.row(0)
    headers = {}
    for col_idx in range(sh.ncols):
        try:
            val = in_header[col_idx].value.strip().lower()
        except Exception:
            continue
        headers[context.lookup_value("headers", val)] = col_idx
    for rx in range(1, sh.nrows):
        drow = row_to_dict(sh.row(rx), headers)
        item_id = drow.pop("id")
        schema = context.lookup_value("type", drow["type"], "LegalEntity")
        entity = context.make(schema)
        names = h.multi_split(drow.pop("name"), ["alias", "ALIAS"])
        entity.id = context.make_id(item_id, *names)
        sanction = h.make_sanction(context, entity)
        sanction.add("authorityId", item_id)
        entity.add("topics", "sanction")
        entity.add("name", names[0])
        entity.add("alias", names[1:])
        if addr := drow.pop("address", None):
            for addr in addr_delim.split(addr):
                h.apply_address(
                    context, entity, h.make_address(context, addr, lang="ind")
                )
        entity.add("country", drow.pop("country", None))
        entity.add("notes", drow.pop("description", None), lang="ind")
        print("xxxxx", entity.schema, entity.caption)
        entity.add_cast("Person", "birthPlace", drow.pop("birth_place", None))
        dob_raw = drow.pop("birth_date", [])
        if dob_raw and dob_raw != "00/00/0000":
            entity.add_schema("Person")
            apply_dob(entity, dob_raw)
        context.emit(entity, target=True)
        context.emit(sanction)
