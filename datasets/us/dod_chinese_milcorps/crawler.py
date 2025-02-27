from zavod import Context, helpers
import csv


def crawl(context: Context) -> None:
    source_file = context.fetch_resource("source.csv", context.data_url)
    with open(source_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entity = context.make("Company")
            name = row.pop("Clean Name")
            entity.id = context.make_id(name)
            entity.add("name", name)
            entity.add("alias", row.pop("Name", None))
            entity.add("alias", row.pop("Alias", None))
            entity.add("notes", row.pop("Note", None))
            parent_name = row.pop("Parent Name", None)
            if parent_name != "" and parent_name != name:
                parent = context.make("Company")
                parent.id = context.make_id(parent_name)
                parent.add("name", parent_name)
                context.emit(parent)

                own = context.make("Ownership")
                own.id = context.make_id("ownership", name, parent_name)
                own.add("owner", parent)
                own.add("asset", entity)
                context.emit(own)
            entity.add("topics", "debarment")
            sanction = helpers.make_sanction(context, entity)
            sanction.add(
                "program",
                "Section 1260H of the William M. (“Mac”) Thornberry National Defense Authorization Act for Fiscal Year 2021 (Public Law 116-283)",
            )
            sanction.add("startDate", row.pop("Start date", None))
            sanction.add("endDate", row.pop("End date", None))
            context.emit(sanction)
            context.emit(entity)
            context.audit_data(row)
