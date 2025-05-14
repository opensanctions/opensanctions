import csv
from pathlib import Path
from zavod.context import Context


LOCAL_PATH = Path(__file__).parent / "dataset.csv"


def crawl(context: Context):

    data_path = context.get_resource_path("source.csv")
    with open(LOCAL_PATH, "r") as fh:
        with open(data_path, "w") as out:
            out.write(fh.read())

    with open(data_path, "r") as fh:
        for row in csv.DictReader(fh):
            entity = context.make(row.pop("schema"))
            entity.id = context.make_slug(list(row.values()))
            entity.add("name", row.pop("name"))
            entity.add("country", row.pop("country"))
            entity.add("idNumber", row.pop("id_number"))
            context.emit(entity)
