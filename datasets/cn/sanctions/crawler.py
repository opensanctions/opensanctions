import csv
import shutil
from pathlib import Path

from rigour.mime.types import CSV
from zavod import Context
from zavod import helpers as h


LOCAL_PATH = Path(__file__).parent


def crawl(context: Context) -> None:
    source_file = LOCAL_PATH / "sanctions.csv"
    resource_path = context.get_resource_path("source.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, context.SOURCE_TITLE)

    with open(source_file, encoding="utf-8", newline="") as f:
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
            entity.add("topics", row.pop("Topics").split(";"))
            program = row.pop("List", None)
            sanction = h.make_sanction(
                context,
                entity,
                program_name=program,
                program_key=h.lookup_sanction_program_key(context, program),
            )
            sanction.set("authority", row.pop("Body", None))
            h.apply_date(sanction, "startDate", row.pop("Date", None))
            h.apply_date(sanction, "endDate", row.pop("End date", None))
            sanction.add("sourceUrl", row.pop("Source URL", None))
            context.emit(sanction)
            context.emit(entity)
            context.audit_data(row)
