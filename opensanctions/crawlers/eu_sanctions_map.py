from opensanctions.core import Context
from opensanctions import helpers as h

DATA_URL = "https://www.sanctionsmap.eu/api/v1/data?"
REGIME_URL = "https://www.sanctionsmap.eu/api/v1/regime"

TYPES = {"E": "LegalEntity", "P": "Person"}


def crawl(context: Context):
    regime = context.fetch_json(REGIME_URL)
    for item in regime["data"]:
        regime_url = f"{REGIME_URL}/{item['id']}"
        regime_data = context.fetch_json(regime_url)["data"]
        measures = regime_data.pop("measures")
        regime_data.pop("legal_acts", None)
        regime_data.pop("general_guidances", None)
        regime_data.pop("guidances", None)
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

                    sanction = h.make_sanction(context, entity, key=regime_data["id"])
                    sanction.set("authority", authority)
                    sanction.set("reason", member["reason"])
                    sanction.add("summary", regime_data["specification"])
                    # context.inspect(id_code)
                    context.emit(entity, target=True)
                    context.emit(sanction)
