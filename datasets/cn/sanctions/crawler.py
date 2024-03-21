import csv

from zavod import Context
from zavod import helpers as h

FORMAT = ["%d.%m.%y"]


def crawl(context: Context) -> None:
    source_file = context.fetch_resource("source.csv", context.data_url)
    with open(source_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entity = context.make(row.pop("Type"))
            name = row.pop("Name")
            if name is None:
                continue
            qid = row.pop("QID", None)
            entity.id = qid or context.make_id(name)
            entity.add("wikidataId", qid)
            entity.add("name", name)
            entity.add("country", row.pop("Country", None))
            entity.add("notes", row.pop("Summary", None))
            entity.add("topics", "poi")
            sanction = h.make_sanction(context, entity)
            sanction.set("authority", row.pop("Body", None))
            sanction.add("program", row.pop("List", None))
            sanction.add("startDate", h.parse_date(row.pop("Date", None), FORMAT))
            sanction.add("sourceUrl", row.pop("Source URL", None))
            context.emit(sanction)
            context.emit(entity, target=True)
            context.audit_data(row)
