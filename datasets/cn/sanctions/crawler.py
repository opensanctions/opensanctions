import csv

from zavod import Context
from zavod import helpers as h


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
            entity.add("name", name, lang="eng")
            entity.add("alias", row.pop("Alias"), lang="eng")
            entity.add("alias", row.pop("Chinese name"), lang="zho")
            entity.add("country", row.pop("Country", None))
            entity.add("notes", row.pop("Summary", None), lang="eng")
            entity.add("notes", row.pop("Chinese summary", None), lang="zho")
            entity.add("topics", "sanction.counter")
            sanction = h.make_sanction(context, entity)
            sanction.set("authority", row.pop("Body", None))
            sanction.add("program", row.pop("List", None))
            h.apply_date(sanction, "startDate", row.pop("Date", None))
            h.apply_date(sanction, "endDate", row.pop("End date", None))
            sanction.add("sourceUrl", row.pop("Source URL", None))
            context.emit(sanction)
            context.emit(entity)
            context.audit_data(row)
