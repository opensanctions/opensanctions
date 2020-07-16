import csv
from pprint import pprint  # noqa
from datetime import datetime
from collections import defaultdict
from normality import stringify
from followthemoney import model
from ftmstore.memorious import EntityEmitter

from opensanctions.util import jointext


def parse_date(date):
    date = stringify(date)
    if date is None:
        return
    date = date.replace("00/00/", "")
    date = date.strip()
    if len(date) == 4:
        return date
    try:
        date = datetime.strptime(date, "%d/%m/%Y")
        return date.date().isoformat()
    except Exception:
        pass
    try:
        date = datetime.strptime(date, "00/%m/%Y")
        return date.date().isoformat()[:7]
    except Exception:
        pass


def split_items(text):
    items = []
    text = stringify(text)
    if text is None:
        return items
    for raw in text.split(")"):
        if " " not in raw:
            items.append(raw)
            continue

        cleaned, suffix = raw.split(" ", 1)
        suffix = suffix.replace("(", "")
        try:
            int(suffix)
            items.append(cleaned)
        except Exception:
            items.append(raw)
    return items


def parse_entry(emitter, group, rows):
    entity = emitter.make("LegalEntity")
    entity.id = "gbhmt-%s" % group
    sanction = emitter.make("Sanction")
    sanction.make_id(entity.id, "Sanction")
    sanction.add("entity", entity)
    sanction.add("authority", "HM Treasury Financial sanctions targets")
    sanction.add("country", "gb")
    for row in rows:
        if row.pop("Group Type") == "Individual":
            entity.schema = model.get("Person")
        row.pop("Alias Type", None)
        name1 = row.pop("Name 1")
        entity.add("firstName", name1, quiet=True)
        name2 = row.pop("Name 2")
        name3 = row.pop("Name 3")
        name4 = row.pop("Name 4")
        name5 = row.pop("Name 5")
        name6 = row.pop("Name 6")
        entity.add("lastName", name6, quiet=True)
        name = jointext(name1, name2, name3, name4, name5, name6)
        if not entity.has("name"):
            entity.add("name", name)
        else:
            entity.add("alias", name)
        entity.add("title", row.pop("Title"), quiet=True)
        sanction.add("program", row.pop("Regime"))
        last_updated = parse_date(row.pop("Last Updated"))
        sanction.add("modifiedAt", last_updated)
        sanction.add("startDate", parse_date(row.pop("Listed On")))
        entity.add("modifiedAt", last_updated)
        entity.add("position", row.pop("Position"), quiet=True)
        entity.add("notes", row.pop("Other Information"), quiet=True)
        entity.add("birthDate", parse_date(row.pop("DOB")), quiet=True)
        entity.add("nationality", row.pop("Nationality", None), quiet=True)

        country = row.pop("Country", None)
        entity.add("country", country)

        address = jointext(
            row.pop("Address 1", None),
            row.pop("Address 2", None),
            row.pop("Address 3", None),
            row.pop("Address 4", None),
            row.pop("Address 5", None),
            row.pop("Address 6", None),
            row.pop("Post/Zip Code", None),
            country,
        )
        entity.add("address", address, quiet=True)

        passport = row.pop("Passport Details", None)
        entity.add("passportNumber", passport, quiet=True)

        national_id = row.pop("NI Number", None)
        entity.add("nationalId", national_id, quiet=True)

        for country in split_items(row.pop("Country of Birth")):
            entity.add("country", country)

        for town in split_items(row.pop("Town of Birth", None)):
            entity.add("birthPlace", town)

    emitter.emit(entity)
    emitter.emit(sanction)


def parse(context, data):
    emitter = EntityEmitter(context)
    groups = defaultdict(list)
    with context.http.rehash(data) as res:
        with open(res.file_path, "r", encoding="iso-8859-1") as csvfile:
            # ignore first line
            next(csvfile)
            for row in csv.DictReader(csvfile):
                group = row.pop("Group ID")
                if group is not None:
                    groups[group].append(row)

    for group, rows in groups.items():
        parse_entry(emitter, group, rows)

    emitter.finalize()
