import csv
from rigour.mime.types import CSV

from zavod import Context, helpers as h

SPLITS = [
    "1: ",
    "2: ",
    "3: ",
    "4: ",
]
URL = "https://main.un.org/securitycouncil/en/sanctions/1718/materials/1718-Designated-Vessels-List"

PROGRAMS = [
    "Paragraph 8 (d) of resolution 1718 (2006) and paragraph 12 of resolution 2270 (2016)",
    "Paragraph 12 (a) of resolution 2321 (2016)",
    "Paragraph 12 (d) of resolution 2321 (2016)",
    "Paragraph 6 of resolution 2371 (2017) and paragraph 6 of resolution 2375 (2017)",
    "Economic resources controlled or operated by Ocean Maritime Management (KPe.020)",
]


def crawl(context: Context):
    # Fetch the HTML and assert the hash of the source URL
    doc = context.fetch_html(URL, cache_days=1)
    source_url = doc.xpath(
        ".//div[a[contains(text(), '1718 Designated Vessels List (Pdf )')]]//a/@href"
    )[0]
    h.assert_url_hash(context, source_url, "739524554d940a60f462b8a20fdda9dc6e4f274c")
    # Fetch the CSV file
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            int_id = row.pop("#")
            name = row.pop("vessel_name")
            imo = row.pop("imo_number")
            designated_owner = row.pop("designated_owner")
            # Create the vessel
            vessel = context.make("Vessel")
            vessel.id = context.make_id(name, imo, int_id)
            vessel.add("name", h.multi_split(name, SPLITS))
            vessel.add("topics", "sanction")
            vessel.add("imoNumber", imo)
            vessel.add("mmsi", h.multi_split(row.pop("mmsi"), SPLITS))
            vessel.add("flag", h.multi_split(row.pop("flag"), SPLITS))
            vessel.add("callSign", h.multi_split(row.pop("call_sign"), SPLITS))
            vessel.add("description", row.pop("other_information"))
            vessel.add("type", row.pop("type"))
            # Create the sanction
            sanction = h.make_sanction(context, vessel)
            h.apply_date(sanction, "startDate", row.pop("date_of_designation"))
            for program in PROGRAMS:
                value = row.pop(program).strip()
                if value and value.lower() != "na":
                    sanction.add("program", program)
            # Create and emit the unknownLink to the designated owner
            if designated_owner:
                linked_entity = context.make("LegalEntity")
                linked_entity.id = context.make_id(designated_owner)
                linked_entity.add("name", designated_owner)
                context.emit(linked_entity)

                link = context.make("UnknownLink")
                link.id = context.make_id(
                    linked_entity.id, "designated As Economic Resources Of", vessel.id
                )
                link.add("object", vessel.id)
                link.add("subject", linked_entity.id)
                link.add("role", "designated As Economic Resources Of")
                context.emit(link)

            context.emit(vessel)
            context.emit(sanction)

            context.audit_data(row, ignore=["image_url"])
