from zavod import helpers as h
from zavod import Context
from lxml.etree import Element
from datetime import datetime
import xlrd
import re

DATETIME_FORMAT = "%Y%m%d%H%M%S"
addr_delim = re.compile(r"[\W][a-zA-Z]\)|;")


def is_sanctions_xlsx(a: Element) -> bool:
    if (
        a.text is not None
        and a.text.strip().lower().find("dttot") > -1
        and (parse_link(a).endswith(".xls") or parse_link(a).endswith(".xlsx"))
    ):
        return True
    return False


def parse_link(a: Element) -> str:
    return a.get("href", "").split("/")[-1]


def find_last_link(context: Context, html: Element) -> Element:
    last_time = datetime.strptime("19700101000000", DATETIME_FORMAT)
    last_link = None
    for link in html.iterdescendants("a"):
        if not is_sanctions_xlsx(link):
            continue
        try:
            ts = parse_link(link).split(".")[0]
            ts = datetime.strptime(ts, DATETIME_FORMAT)
        except Exception:
            context.log.error(f"Could not parse timestamp from {link.get('href')}")
            continue
        if ts > last_time:
            last_time = ts
            last_link = link
    return last_link


def value_or_none(row, headers, key):
    try:
        return row[headers[key]].value
    except Exception:
        return None


def parse_dates(value: str):
    """Dates come in arbitrary formats, but the most common ones seem to be
    01/04/1983 and 01 apr 1983. Sometimes there are multiple dates in the same cell.
    """
    pat = re.compile(r"(\d{1,2} [a-zA-Z]{3,9} \d{4})")
    pat_slash_sep = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})")
    for match in pat_slash_sep.finditer(value):
        yield match.group()
    for match in pat.finditer(value):
        yield match.group()


def row_to_dict(row, headers: dict) -> dict:
    return {k: row[headers[k]].value for k in headers if value_or_none(row, headers, k)}


def get_schema(context: Context, row, headers):
    schema = context.lookup_value("type", row[headers["type"]].value)
    if schema:
        return schema
    return "LegalEntity"


def crawl(context: Context):
    html = context.fetch_html(context.data_url)
    last_link = find_last_link(context, html)
    xls = context.fetch_resource("source.xls", last_link.get("href"))
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
        row = sh.row(rx)
        drow = row_to_dict(sh.row(rx), headers)
        entity = context.make(get_schema(context, row, headers))
        names = h.multi_split(drow.pop("name"), ["alias", "ALIAS"])
        entity.id = context.make_id(drow.pop("id"), *names)
        sanction = h.make_sanction(context, entity)
        entity.add("topics", "sanction")
        sanction.add("program", "DTTOT")
        entity.add("name", names[0])
        entity.add("alias", names[1:])
        if addr := drow.pop("address", None):
            for addr in addr_delim.split(addr):
                h.apply_address(
                    context, entity, h.make_address(context, addr, lang="ind")
                )
        entity.add("country", drow.pop("country", None))
        entity.add("description", drow.pop("description", None), lang="ind")
        if str(entity.schema) == "Person":
            entity.add("birth_place", drow.pop("birth_place", None))
            for dob in parse_dates(drow.pop("birth_date", [])):
                entity.add("birth_date", h.parse_date(dob))
        context.emit(entity, target=True)
        context.emit(sanction)
