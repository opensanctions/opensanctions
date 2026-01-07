import openpyxl

from zavod import Context
from zavod import helpers as h

DATA_URL = "https://www.sanctionsmap.eu/api/v1/data?"
REGIME_URL = "https://www.sanctionsmap.eu/api/v1/regime"

# Why do we parse vessels separately?
#   > Entities contained in Annex IV of Council Regulation (EU) No 833/2014 are subject to
#   > specific economic prohibitions as referred to in Article 2(7), 2a(7) and 2b(1) of
#   > Regulation (EU) No 833/2014. They are, however, not subject to an asset freeze, hence
#   > not included in the Consolidated list.
# All sanctioned vessels meeting this definition are provided in a separate XLSX file.

# VESSELS_URL originates from the EU Sanctions Map: https://www.sanctionsmap.eu/
#
# How to find the URL:
#
# 1. Go to https://www.sanctionsmap.eu/ and click the download button
#    for the "Consolidated list of designated vessels" Excel file.
#
# 2. A new tab opens and the file dowloads. Right click in your download
#    list and select "Copy Download Link". Alternatively, search the active JS bundles
#    for `cloudfront.net` to find the `.xlsx` URL.
#
# Same list is also published by the Danish Maritime Authority:
# https://www.dma.dk/growth-and-framework-conditions/maritime-sanctions/sanctions-against-russia-and-belarus/eu-vessel-designations
# Note: the DMA list does not include links to the Official Journal.

VESSELS_URL = (
    "https://dk9q89lxhn3e0.cloudfront.net/EU+designated+vessels-+conso+July+2025.xlsx"
)
PROGRAM_KEY = "EU-MARE"
TYPES = {"E": "LegalEntity", "P": "Person"}


def crawl_regime(context):
    regime = context.fetch_json(REGIME_URL, cache_days=1)
    for item in regime["data"]:
        regime_url = f"{REGIME_URL}/{item['id']}"
        regime_data = context.fetch_json(regime_url, cache_days=1)["data"]
        measures = regime_data.pop("measures")
        regime_data.pop("legal_acts", None)
        regime_data.pop("general_guidances", None)
        regime_data.pop("guidances", None)
        programme = regime_data.pop("programme")
        authority = regime_data["adopted_by"]["data"]["title"]

        for measure in measures["data"]:
            for measure_list in measure["lists"]["data"]:
                for member in measure_list["members"]["data"]:
                    if "FSD_ID" not in member:
                        member = member["data"]
                    if member["FSD_ID"] is not None:
                        continue
                    schema = TYPES[member["type"]]
                    name = member["name"]
                    id_code = member["id_code"]
                    if id_code is not None and "IMO:" in id_code:
                        schema = "Vessel"
                    if id_code == "8405311":
                        schema = "Vessel"

                    entity = context.make(schema)
                    entity.id = context.make_id(name, member["creation_date"])
                    entity.add("topics", "sanction")

                    if "(alias" in name:
                        name, _ = name.split("(alias", 1)

                    if "\n" in name:
                        name, notes = name.split("\n", 1)
                        entity.add("notes", notes)

                    entity.add("name", name)

                    if not entity.schema.is_a("Vessel"):
                        entity.add("notes", id_code)
                    else:
                        for code in id_code.split("."):
                            if ":" not in code:
                                entity.add("imoNumber", code)
                                continue
                            type_, value = code.split(": ", 1)
                            if "IMO" in type_:
                                entity.add("imoNumber", value)
                            if "MMSI" in type_:
                                entity.add("mmsi", value)
                    for prog in programme:
                        sanction = h.make_sanction(
                            context,
                            entity,
                            key=regime_data["id"],
                            source_program_key=prog,
                            program_name=prog,
                            program_key=h.lookup_sanction_program_key(context, prog),
                        )
                    sanction.set("authority", authority)
                    sanction.set("reason", member["reason"])
                    sanction.add("summary", regime_data["specification"])
                    # context.inspect(id_code)
                    context.emit(entity)
                    context.emit(sanction)


def crawl_vessels(context):
    path = context.fetch_resource("vessels.xlsx", VESSELS_URL)
    workbook: openpyxl.Workbook = openpyxl.load_workbook(
        path, read_only=True, data_only=True
    )
    assert workbook.sheetnames == ["Sheet1"]
    for row in h.parse_xlsx_sheet(
        context,
        sheet=workbook["Sheet1"],
    ):
        name = row.pop("vessel_name")
        imo = row.pop("imo_number")
        order_id = row.pop("column_0")
        if not any([name, imo, order_id]):
            continue
        vessel = context.make("Vessel")
        vessel.id = context.make_id(name, imo)
        vessel.add("name", name)
        vessel.add("imoNumber", imo)
        vessel.add("topics", "sanction")
        sanction = h.make_sanction(
            context, vessel, key=order_id, program_key=PROGRAM_KEY
        )
        sanction.add("sourceUrl", row.pop("link_to_relevant_eu_official_journal"))
        h.apply_date(sanction, "startDate", row.pop("date_of_application"))
        context.emit(vessel)
        context.emit(sanction)
        context.audit_data(row)


def crawl(context: Context):
    crawl_regime(context)
    crawl_vessels(context)
