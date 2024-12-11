import csv
from rigour.mime.types import CSV

from zavod import Context, helpers as h

SPLITS = [
    "1. ",
    "2. ",
    "3. ",
    "1: ",
    "2: ",
    "3: ",
    "4: ",
]


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            int_id = row.pop("#")
            name = row.pop("vessel name")
            imo = row.pop("IMO number")
            p8d_1718 = row.pop(
                "paragraph 8 (d) of resolution 1718 (2006) and paragraph 12 of resolution 2270 (2016)"
            )
            p12a_2321 = row.pop("paragraph 12 (a) of resolution 2321 (2016)")
            p12d_2321 = row.pop("paragraph 12 (d) of resolution 2321 (2016)")
            p6_2371 = row.pop(
                "paragraph 6 of resolution 2371 (2017) and paragraph 6 of resolution 2375 (2017)"
            )
            oom_econ = row.pop(
                "economic resources controlled or operated by Ocean Maritime Management (KPe.020)"
            )
            picture = row.pop("picture link")
            designated_name = row.pop("designated as economic resources of")

            vessel = context.make("Vessel")
            vessel.id = context.make_id(name, imo, int_id)
            vessel.add("name", name)
            vessel.add("imoNumber", imo)
            vessel.add("mmsi", h.multi_split(row.pop("MMSI"), SPLITS))
            vessel.add("flag", h.multi_split(row.pop("flag"), SPLITS))
            vessel.add("callSign", row.pop("call sign"))
            vessel.add("description", row.pop("other information"))
            vessel.add("type", row.pop("type"))

            sanction = h.make_sanction(context, vessel)
            h.apply_date(sanction, "startDate", row.pop("date of designation"))

            if designated_name is not None:
                linked = context.make("LegalEntity")
                linked.id = context.make_id(designated_name)
                linked.add("name", designated_name)

                link = context.make("UnknownLink")
                link.id = context.make_id(vessel.id, linked.id)

            context.emit(vessel, target=True)
            context.emit(sanction)

            context.audit_data(row)
