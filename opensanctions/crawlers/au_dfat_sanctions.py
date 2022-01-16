import xlrd
import string
from collections import defaultdict
from normality import slugify
from datetime import datetime
from followthemoney import model
from pantomime.types import EXCEL

from opensanctions import helpers as h
from opensanctions.core import Context
from opensanctions.util import multi_split, remove_bracketed

SPLITS = [" %s)" % char for char in string.ascii_lowercase]
FORMATS = ["%d/%m/%Y", "%d %b. %Y", "%d %b.%Y", "%d %b %Y", "%d %B %Y"]
FORMATS = FORMATS + ["%b. %Y", "%d %B. %Y", "%Y"]


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
    date = remove_bracketed(date)
    if date is None:
        return dates
    date = date.replace("\n", " ")
    for part in multi_split(date, splits):
        part = part.strip().strip(",")
        if not len(part):
            continue
        dates.update(h.parse_date(part, FORMATS))
    return dates


def clean_reference(ref):
    if isinstance(ref, (int, float)):
        return int(ref)
    number = ref
    while len(number):
        try:
            return int(number)
        except Exception:
            number = number[:-1]
    raise ValueError()


def parse_reference(context: Context, reference, rows):
    entity = context.make("LegalEntity")
    entity.id = context.make_slug(reference)
    # entity.add("sourceUrl", context.dataset.url)
    sanction = h.make_sanction(context, entity)

    for row in rows:
        if row.pop("type") == "Individual":
            entity.schema = model.get("Person")

        name = row.pop("name_of_individual_or_entity", None)
        if row.pop("name_type") == "aka":
            entity.add("alias", name)
        else:
            entity.add("name", name)

        addr = row.pop("address")
        if addr is not None:
            for part in multi_split(addr, SPLITS):
                address = h.make_address(context, full=part)
                h.apply_address(context, entity, address)
        sanction.add("program", row.pop("committees"))
        citizen = multi_split(row.pop("citizenship"), ["a)", "b)", "c)", "d)"])
        entity.add("nationality", citizen, quiet=True)
        dates = clean_date(row.pop("date_of_birth"))
        entity.add("birthDate", dates, quiet=True)
        entity.add("birthPlace", row.pop("place_of_birth"), quiet=True)
        entity.add("notes", row.pop("additional_information"))
        entity.add("notes", row.pop("listing_information"), quiet=True)

        control_date = row.pop("control_date")
        sanction.add("modifiedAt", control_date)
        entity.add("modifiedAt", control_date)

    entity.add("topics", "sanction")
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.xls", context.dataset.data.url)
    context.export_resource(path, EXCEL, title=context.SOURCE_TITLE)
    xls = xlrd.open_workbook(path)
    ws = xls.sheet_by_index(0)
    headers = [slugify(h, sep="_") for h in ws.row_values(0)]
    references = defaultdict(list)
    for r in range(1, ws.nrows):
        row = [h.convert_excel_cell(xls, c) for c in ws.row(r)]
        row = dict(zip(headers, row))
        reference = clean_reference(row.get("reference"))
        references[reference].append(row)

    for ref, rows in references.items():
        parse_reference(context, ref, rows)
