import string
import openpyxl
from typing import List, Optional, Set, Tuple, Dict, Any
from collections import defaultdict
from normality import slugify
from datetime import datetime
from pantomime.types import XLSX

from zavod import Context
from zavod import helpers as h

SPLITS = [" %s)" % char for char in string.ascii_lowercase]
FORMATS = ["%d/%m/%Y", "%d %b. %Y", "%d %b.%Y", "%d %b %Y", "%d %B %Y"]
FORMATS = FORMATS + ["%b. %Y", "%d %B. %Y", "%Y", "%m/%Y"]


def clean_date(date):
    splits = [
        "a)",
        "b)",
        "c)",
        "d)",
        "e)",
        "f)",
        "g)",
        "h)",
        "i)",
        " or ",
        " to ",
        " and ",
        "alt DOB:",
        "alt DOB",
        ";",
        ">>",
    ]
    dates = set()
    if isinstance(date, float):
        date = str(int(date))
    if isinstance(date, datetime):
        date = date.date().isoformat()
    if isinstance(date, int):
        date = str(date)
    date = h.remove_bracketed(date)
    if date is None:
        return dates
    date = date.replace("\n", " ")
    for part in h.multi_split(date, splits):
        part = part.strip().strip(",")
        if not len(part):
            continue
        dates.update(h.parse_date(part, FORMATS))
    return dates


def clean_reference(ref: str) -> Optional[int]:
    number = ref
    while len(number):
        try:
            return int(number)
        except Exception:
            number = number[:-1]
    raise ValueError()


def clean_country(country: str) -> List[str]:
    return h.multi_split(country, ["a)", "b)", "c)", "d)", ";", ",", " and "])


def parse_reference(
    context: Context, reference: int, rows: List[Dict[str, Any]]
) -> None:
    schemata = set()
    for row in rows:
        type_ = row.pop("type")
        schema = context.lookup_value("type", type_)
        if schema is None:
            context.log.warning("Unknown entity type", type=type_)
            return
        schemata.add(schema)
    if len(schemata) > 1:
        context.log.error(
            "Multiple entity types",
            schemata=list(schemata),
            reference=reference,
            # rows=rows,
        )
        return
    entity = context.make(schemata.pop())

    primary_name: Optional[str] = None
    names: List[Tuple[str, str]] = []
    for row in rows:
        name = row.pop("name_of_individual_or_entity", None)
        name_type = row.pop("name_type")
        name_prop = context.lookup_value("name_type", name_type)
        if name_prop is None:
            context.log.warning("Unknown name type", name_type=name_type)
            return
        names.append((name_prop, name))
        if name_prop == "name" or primary_name is None:
            primary_name = name

    entity.id = context.make_slug(reference, primary_name)
    for name_prop, name in names:
        entity.add(name_prop, name)
    sanction = h.make_sanction(context, entity)

    primary_name = None
    for row in rows:
        addr = row.pop("address")
        if addr is not None:
            for part in h.multi_split(addr, SPLITS):
                address = h.make_address(context, full=part)
                h.apply_address(context, entity, address)
        sanction.add("program", row.pop("committees"))
        country = clean_country(row.pop("citizenship"))
        if entity.schema.is_a("Person"):
            entity.add("nationality", country)
        else:
            entity.add("country", country)
        dates = clean_date(row.pop("date_of_birth"))
        entity.add("birthDate", dates, quiet=True)
        entity.add("birthPlace", row.pop("place_of_birth"), quiet=True)
        entity.add("notes", h.clean_note(row.pop("additional_information")))
        listing_info = row.pop("listing_information")
        if isinstance(listing_info, datetime):
            entity.add("createdAt", listing_info)
            sanction.add("listingDate", listing_info)
        else:
            sanction.add("summary", listing_info)
        # TODO: consider parsing if it's not a datetime?

        control_date = row.pop("control_date")
        sanction.add("startDate", control_date)
        entity.add("createdAt", control_date)
        context.audit_data(row, ignore=["reference"])

    entity.add("topics", "sanction")
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    references = defaultdict(list)
    raw_references: Set[str] = set()
    # duplicates = set()
    for sheet in workbook.worksheets:
        headers: Optional[List[str]] = None
        for row in sheet.rows:
            cells = [c.value for c in row]
            if headers is None:
                headers = [slugify(h, sep="_") for h in cells]
                continue
            row = dict(zip(headers, cells))
            raw_ref = row.get("reference")
            if raw_ref is None:
                continue
            raw_ref = str(raw_ref)
            if raw_ref in raw_references:
                raise ValueError("Duplicate reference: %s" % raw_ref)
                # duplicates.add(raw_ref)

            raw_references.add(raw_ref)
            reference = clean_reference(raw_ref)
            if reference is not None:
                references[reference].append(row)

    # xls = xlrd.open_workbook(path)
    # ws = xls.sheet_by_index(0)
    # headers = [slugify(h, sep="_") for h in ws.row_values(0)]
    # references = defaultdict(list)
    # for r in range(1, ws.nrows):
    #     cells = [h.convert_excel_cell(xls, c) for c in ws.row(r)]
    #     row = dict(zip(headers, cells))
    #     reference = clean_reference(row.get("reference"))
    #     references[reference].append(row)

    for ref, rows in references.items():
        parse_reference(context, ref, rows)

    # print("Duplicates:", duplicates)
