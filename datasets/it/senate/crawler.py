import json
from rigour.mime.types import JSON

from zavod.stateful.positions import categorise
from zavod import Context, helpers as h


DATA = {
    "alias": "senatori-legislatura",
    "id": "2",
    "legislatura": "19",
    "search[legislatura]": "19",
    "query_format": "json",
    "commit": "Download",
}
IGNORE = [
    "tipoMandato",  # mandate_type
    "tipoFineMandato",  # end_mandate_type
    "provinciaNascita",  # birth_province
    "legislatura",  # legislature
]


def check_next_legislature(context: Context):
    """Check if the next legislature is already available."""
    DATA["legislatura"] = "20"
    DATA["search[legislatura]"] = "20"
    path = context.fetch_resource(
        "next_legislature.json", context.data_url, method="POST", data=DATA
    )
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    count = len(data["results"]["bindings"])
    if count > 200:
        context.log.warning(
            f"Next legislature has {count} senators, update the crawler to include it."
        )


def crawl(context: Context):
    check_next_legislature(context)
    path = context.fetch_resource(
        "source.json", context.data_url, method="POST", data=DATA
    )
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)
    print(f"Found {len(data["results"]["bindings"])} senators in the source data.")
    assert len(data["results"]["bindings"]) > 200
    for item in data["results"]["bindings"]:
        first_name = item.pop("nome").get("value")
        last_name = item.pop("cognome").get("value")
        dob = item.pop("dataNascita").get("value")

        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, dob)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
        entity.add("sourceUrl", item.pop("senatore").get("value"))
        entity.add("birthPlace", item.pop("cittaNascita").get("value"))
        entity.add("birthCountry", item.pop("nazioneNascita").get("value"))
        entity.add("gender", item.pop("sesso", {}).get("value"))
        entity.add("citizenship", "it")
        h.apply_date(entity, "birthDate", dob)

        position = h.make_position(
            context,
            name="Member of the Senate",
            wikidata_id="Q13653224",
            country="it",
            topics=["gov.legislative", "gov.national"],
            lang="eng",
        )
        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue

        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            start_date=item.pop("inizioMandato").get("value"),
            end_date=item.pop("fineMandato", {}).get("value"),  # can be None
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(position)
            context.emit(entity)

        context.audit_data(item, IGNORE)
