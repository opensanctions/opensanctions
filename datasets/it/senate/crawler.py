import json
from typing import Any, Dict

from rigour.mime.types import JSON
from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h


def build_request_data(legislature: int) -> Dict[str, str]:
    return {
        "alias": "senatori-legislatura",
        "id": "2",
        "query_format": "json",
        "commit": "Download",
        "legislatura": str(legislature),
        "search[legislatura]": str(legislature),
    }


def crawl_item(context: Context, item: Dict[str, Any]) -> None:
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
        return

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

    context.audit_data(
        item,
        ignore=[
            "tipoMandato",  # mandate_type
            "tipoFineMandato",  # end_mandate_type
            "provinciaNascita",  # birth_province
            "legislatura",  # legislature
        ],
    )


def get_current_legislature(context: Context) -> int:
    # We start crawling from the 18th, which is the previous one at the time of writing.
    current_legislature = 18
    while True:
        assert current_legislature < 30, "We probably ended up in an endless loop"

        data = context.fetch_json(
            context.data_url,
            method="POST",
            data=build_request_data(current_legislature + 1),
        )
        num_senators = len(data["results"]["bindings"])
        if num_senators > 0:
            context.log.info(
                f"Found {num_senators} senators in "
                ""
                f"""legislature {current_legislature + 1}, trying next one."""
            )
            current_legislature += 1
        else:
            context.log.info(
                f"""No senators found in legislature {current_legislature + 1}, """
                f"""so {current_legislature} is the current one."""
            )
            break
    return current_legislature


def crawl(context: Context) -> None:
    current_legislature = get_current_legislature(context)
    last_two_legislatures = [current_legislature - 1, current_legislature]

    for leg in last_two_legislatures:
        context.log.info(f"Crawling legislature {leg}")
        path = context.fetch_resource(
            f"senatori_legislatura_{leg}.json",
            context.data_url,
            method="POST",
            data=build_request_data(leg),
        )
        context.export_resource(path, JSON, title=context.SOURCE_TITLE)
        with open(path, "r") as fh:
            data = json.load(fh)
        for item in data["results"]["bindings"]:
            crawl_item(context, item)
