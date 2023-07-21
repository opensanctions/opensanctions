from typing import Dict, List, Optional
import re
import xlrd
import string
from datetime import datetime
from urllib.parse import urljoin
from pantomime.types import XLS
from normality import collapse_spaces, stringify
from normality.cleaning import decompose_nfkd

from zavod import settings
from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.util import multi_split

BRACKETED = re.compile(r"(\([^\(\)]*\)|\[[^\[\]]*\])")

SPLITS = ["(%s)" % char for char in string.ascii_lowercase]
SPLITS = SPLITS + ["（%s）" % char for char in string.ascii_lowercase]
# WTF full-width brackets?
SPLITS = SPLITS + ["（a）", "（b）", "（c）", "\n"]

# DATE FORMATS
FORMATS = ["%Y年%m月%d日", "%Y年%m月%d", "%Y年%m月", "%Y.%m.%d"]
DATE_SPLITS = SPLITS + ["、", "；", "又は", "または", "生", "改訂", "日", "及び"]
DATE_CLEAN = re.compile(r"(\(|\)|（|）| |改訂日|改訂)")


def parse_date(text: List[Optional[str]]) -> List[str]:
    dates: List[str] = []
    for date in multi_split(text, DATE_SPLITS):
        cleaned = DATE_CLEAN.sub("", date)
        normal = decompose_nfkd(cleaned)
        for parsed in h.parse_date(normal, FORMATS, default=date):
            dates.append(parsed)
    return dates


def parse_names(names: List[str]) -> List[str]:
    cleaned = []
    for name in names:
        name = name.replace("(original script:", "")
        name = name.replace("(a.k.a.:", "")
        name = name.replace("(a.k.a:", "")
        name = name.replace("(previously listed as", "")
        # name = name.replace(")", "")
        cleaned.append(name)
        no_brackets = BRACKETED.sub(" ", name).strip()
        if no_brackets != name and len(no_brackets):
            cleaned.append(name)
    return cleaned


def fetch_xls_url(context: Context) -> str:
    params = {"_": context.data_time.date().isoformat()}
    doc = context.fetch_html(context.data_url, params=params)
    for link in doc.findall('.//div[@class="unique-block"]//a'):
        href = urljoin(context.data_url, link.get("href"))
        if href.endswith(".xls"):
            return href
    context.log.error("Could not find XLS file on MoF web site")


def emit_row(context: Context, sheet: str, section: str, row: Dict[str, List[str]]):
    schema = context.lookup_value("schema", section)
    if schema is None:
        context.log.warning("No schema for section", section=section, sheet=sheet)
        return
    entity = context.make(schema)
    name_english = row.pop("name_english")
    name_japanese = row.pop("name_japanese")
    entity.id = context.make_id(*name_english, *name_japanese)
    if entity.id is None:
        # context.inspect((sheet, row))
        return
    entity.add("name", parse_names(name_english), lang="eng")
    entity.add("name", parse_names(name_japanese))
    entity.add("alias", parse_names(row.pop("alias", [])))
    entity.add("alias", parse_names(row.pop("known_alias", [])))
    entity.add("weakAlias", parse_names(row.pop("weak_alias", [])))
    entity.add("weakAlias", parse_names(row.pop("nickname", [])))
    entity.add("previousName", parse_names(row.pop("past_alias", [])))
    entity.add("previousName", parse_names(row.pop("old_name", [])))
    entity.add_cast("Person", "position", row.pop("position", []), lang="eng")
    birth_date = parse_date(row.pop("birth_date", []))
    entity.add_cast("Person", "birthDate", birth_date)
    entity.add_cast("Person", "birthPlace", row.pop("birth_place", []))
    entity.add_cast("Person", "passportNumber", row.pop("passport_number", []))
    entity.add("idNumber", row.pop("id_number", []))
    entity.add("idNumber", row.pop("identification_number", []))
    entity.add("notes", h.clean_note(row.pop("other_information", None)))
    entity.add("notes", h.clean_note(row.pop("details", None)))
    entity.add("phone", row.pop("phone", []))
    entity.add("phone", row.pop("fax", []))

    for address_full in row.pop("address", []):
        address = h.make_address(context, full=address_full)
        h.apply_address(context, entity, address)

    for address_full in row.pop("where", []):
        address = h.make_address(context, full=address_full)
        h.apply_address(context, entity, address)

    title = row.pop("title", [])
    if entity.schema.is_a("Person"):
        entity.add("title", title)
    else:
        entity.add("notes", title)
    entity.add("country", row.pop("citizenship", []))
    entity.add("country", row.pop("activity_area", []))

    sanction = h.make_sanction(context, entity)
    sanction.add("program", section)
    sanction.add("reason", row.pop("root_nomination", None))
    sanction.add("reason", row.pop("reason_res1483", None))
    sanction.add("authorityId", row.pop("notification_number", None))
    sanction.add("unscId", row.pop("designated_un", None))

    sanction.add("startDate", parse_date(row.pop("notification_date", [])))
    sanction.add("startDate", parse_date(row.pop("designated_date", [])))
    sanction.add("listingDate", parse_date(row.pop("publication_date", [])))

    # if len(row):
    #     context.inspect(row)
    entity.add("topics", "sanction")
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    xls_url = fetch_xls_url(context)
    path = context.fetch_resource("source.xls", xls_url)
    context.export_resource(path, XLS, title=context.SOURCE_TITLE)

    xls = xlrd.open_workbook(path)
    for sheet in xls.sheets():
        headers = None
        row0 = [h.convert_excel_cell(xls, c) for c in sheet.row(0)]
        sections = [c for c in row0 if c is not None]
        section = collapse_spaces(" / ".join(sections))
        for r in range(1, sheet.nrows):
            row = [h.convert_excel_cell(xls, c) for c in sheet.row(r)]

            # after a header is found, read normal data:
            if headers is not None:
                data: Dict[str, List[str]] = {}
                for header, cell in zip(headers, row):
                    if header is None:
                        continue
                    values = []
                    if isinstance(cell, datetime):
                        cell = cell.date()
                    for value in multi_split(stringify(cell), SPLITS):
                        if value is None:
                            continue
                        if value == "不明":
                            continue
                        if value is not None:
                            values.append(value)
                    data[header] = values
                emit_row(context, sheet.name, section, data)

            if not len(row) or row[0] is None:
                continue
            teaser = row[0].strip()
            # the first column of the common headers:
            if "告示日付" in teaser:
                if headers is not None:
                    context.log.error("Found double header?", row=row)
                # print("SHEET", sheet, row)
                headers = []
                for cell in row:
                    cell = collapse_spaces(cell)
                    header = context.lookup_value("columns", cell)
                    if header is None:
                        context.log.warning(
                            "Unknown column title", column=cell, sheet=sheet.name
                        )
                    headers.append(header)
