import xlrd

# from xlrd.xldate import xldate_as_datetime
from collections import defaultdict
from pprint import pprint  # noqa
from datetime import datetime
from normality import slugify
from followthemoney import model


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
    entity.make_id("AUDFAT", reference)
    entity.add("sourceUrl", context.dataset.url)
    sanction = context.make("Sanction")
    sanction.make_id("Sanction", entity.id)
    sanction.add("authority", context.dataset.publisher.title)
    sanction.add("entity", entity)

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
        entity.add("nationality", row.pop("citizenship"), quiet=True)
        entity.add("birthDate", row.pop("date_of_birth"), quiet=True)
        entity.add("birthPlace", row.pop("place_of_birth"), quiet=True)
        entity.add("status", row.pop("listing_information"), quiet=True)

        control_date = int(row.pop("control_date"))
        base_date = datetime(1900, 1, 1).toordinal()
        dt = datetime.fromordinal(base_date + control_date - 2)
        sanction.add("modifiedAt", dt)
        entity.add("modifiedAt", dt)
        entity.context["updated_at"] = dt.isoformat()

    context.emit(entity)
    context.emit(sanction)


def crawl(context):
    context.fetch_artifact("source.xls", context.dataset.data.url)
    xls = xlrd.open_workbook(context.get_artifact_path("source.xls"))
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
            else:
                row[header] = cell.value

        reference = clean_reference(row.get("reference"))
        references[reference].append(row)

    for ref, rows in references.items():
        parse_reference(context, ref, rows)
