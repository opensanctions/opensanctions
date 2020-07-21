import csv
from pprint import pprint  # noqa
from datetime import datetime
from ftmstore.memorious import EntityEmitter

from opensanctions.util import jointext


def parse_date(text):
    if text is None:
        return
    try:
        return datetime.strptime(text, "%m/%d/%Y").date()
    except ValueError:
        return text


def parse_row(emitter, row):
    entity = emitter.make("LegalEntity")
    entity.make_id("USBIS", row.get("Effective_Date"), row.get("Name"))
    entity.add("name", row.get("Name"))
    entity.add("notes", row.get("Action"))
    entity.add("country", row.get("Country"))
    entity.add("modifiedAt", row.get("Last_Update"))
    entity.context["updated_at"] = row.get("Last_Update")

    address = jointext(
        row.get("Street_Address"),
        row.get("Postal_Code"),
        row.get("City"),
        row.get("State"),
        sep=", ",
    )
    entity.add("address", address)
    emitter.emit(entity)

    sanction = emitter.make("Sanction")
    sanction.make_id(entity.id, row.get("FR_Citation"))
    sanction.add("entity", entity)
    sanction.add("program", row.get("FR_Citation"))
    sanction.add("authority", "US Bureau of Industry and Security")
    sanction.add("country", "us")
    sanction.add("startDate", parse_date(row.get("Effective_Date")))
    sanction.add("endDate", parse_date(row.get("Expiration_Date")))
    pprint(row)
    emitter.emit(sanction)


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        with open(res.file_path, "r") as csvfile:
            for row in csv.DictReader(csvfile, delimiter="\t"):
                parse_row(emitter, row)
    emitter.finalize()
