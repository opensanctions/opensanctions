import xlrd
from xlrd.xldate import xldate_as_datetime
from collections import defaultdict
from pprint import pprint  # noqa
from normality import slugify
from datetime import datetime
from followthemoney import model
from prefixdate import parse_formats

from opensanctions.helpers import make_sanction
from opensanctions.util import multi_split, remove_bracketed

FORMATS = ["%d/%m/%Y", "%d %b. %Y", "%d %b.%Y", "%d %b %Y", "%d %B %Y"]
FORMATS = FORMATS + ["%b. %Y", "%d %B. %Y"]


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
        ";",
        ">>",
    ]
    dates = set()
    if isinstance(date, float):
        date = str(int(date))
    if isinstance(date, datetime):
        date = date.date().isoformat()
    date = remove_bracketed(date)
    date = date.replace("\n", " ")
    for part in multi_split(date, splits):
        part = part.lower()
        for word in (
            "approximately",
            "approx",
            "circa",
            "between",
            "alt dob:",
            "alt dob",
            "c.",
            "(.)",
        ):
            part = part.replace(word, "")
        part = part.replace(" de ", " dec ")
        part = part.strip(",")
        part = part.strip()
        part = parse_formats(part, FORMATS)
        dates.add(part)
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


def parse_reference(context, reference, rows):
    entity = context.make("LegalEntity")
    entity.make_slug(reference)
    entity.add("sourceUrl", context.dataset.url)
    sanction = make_sanction(entity)

    for row in rows:
        if row.pop("type") == "Individual":
            entity.schema = model.get("Person")

        name = row.pop("name_of_individual_or_entity", None)
        if row.pop("name_type") == "aka":
            entity.add("alias", name)
        else:
            entity.add("name", name)

        entity.add("address", row.pop("address"))
        entity.add("notes", row.pop("additional_information"))
        sanction.add("program", row.pop("committees"))
        citizen = multi_split(row.pop("citizenship"), ["a)", "b)", "c)", "d)"])
        entity.add("nationality", citizen, quiet=True)
        dates = clean_date(row.pop("date_of_birth"))
        entity.add("birthDate", dates, quiet=True)
        entity.add("birthPlace", row.pop("place_of_birth"), quiet=True)
        entity.add("status", row.pop("listing_information"), quiet=True)

        control_date = row.pop("control_date")
        sanction.add("modifiedAt", control_date)
        entity.add("modifiedAt", control_date)
        entity.context["updated_at"] = control_date.isoformat()

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context):
    context.fetch_resource("source.xls", context.dataset.data.url)
    xls = xlrd.open_workbook(context.get_resource_path("source.xls"))
    ws = xls.sheet_by_index(0)
    headers = [slugify(h, sep="_") for h in ws.row_values(0)]
    references = defaultdict(list)
    for r in range(1, ws.nrows):
        row = ws.row(r)
        row = dict(zip(headers, row))
        for header, cell in row.items():
            if cell.ctype == 2:
                row[header] = str(int(cell.value))
            elif cell.ctype == 0:
                row[header] = None
            if cell.ctype == 3:
                dt = xldate_as_datetime(cell.value, xls.datemode)
                row[header] = dt
            else:
                row[header] = cell.value

        reference = clean_reference(row.get("reference"))
        references[reference].append(row)

    for ref, rows in references.items():
        parse_reference(context, ref, rows)
