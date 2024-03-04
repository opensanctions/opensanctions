from zavod import Context
import csv


def crawl(context: Context):
    source_file = context.fetch_resource("source.csv", context.dataset.data.url)
    with open(source_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entity = context.make("Company")
            name = row.pop("Clean Name")
            entity.id = context.make_id(name)
            entity.add("name", name)
            entity.add("alias", row.pop("Alias", None))
            entity.add("notes", row.pop("Note", None))
            parent_name = row.pop("Parent Name", None)
            if parent_name != "" and parent_name != name:
                parent = context.make("Company")
                parent.id = context.make_id(parent_name)
                parent.add("name", parent_name)
                context.emit(parent)
                entity.add("parent", parent)
            entity.add("topics", "debarment")
            context.emit(entity, target=True)
            context.audit_data(row, ignore=["Name"])
