from typing import Any, Dict

from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h

# The query retrieves a list of members of
# the Italian Chamber of Deputies (Camera dei Deputati).
#
# Query builder: https://dati.camera.it/sparql

QUERY = """
SELECT DISTINCT
  ?persona ?cognome ?nome ?info
  ?dataNascita ?nato ?luogoNascita ?genere
  ?inizioMandato ?fineMandato
  ?nomeGruppo ?sigla ?aggiornamento
WHERE {{
  # Person entity (a member of parliament)
  ?persona ocd:rif_mandatoCamera ?mandato;
           a foaf:Person.

  # Deputy membership, group affiliation, and legislature
  ?d a ocd:deputato;
     ocd:aderisce ?aderisce;
     ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_{legislature}>;
     ocd:rif_mandatoCamera ?mandato.
  OPTIONAL {{ ?d dc:description ?info }}

  # Personal information (name, gender, birth details)
  ?d foaf:surname ?cognome;
     foaf:gender ?genere;
     foaf:firstName ?nome.

  OPTIONAL {{
    ?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
    ?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita;
              rdfs:label ?nato;
              ocd:rif_luogo ?luogoNascitaUri.
    ?luogoNascitaUri dc:title ?luogoNascita.
  }}

  # System update timestamp (last modified)
  OPTIONAL {{ ?d <http://lod.xdams.org/ontologies/ods/modified> ?aggiornamento. }}

  # Mandate details (start and end dates)
  ?mandato ocd:rif_elezione ?elezione.
  OPTIONAL {{ ?mandato ocd:endDate ?fineMandato. }}
  OPTIONAL {{ ?mandato ocd:startDate ?inizioMandato. }}

  # Political group affiliation (name and abbreviation)
  OPTIONAL {{
    ?aderisce ocd:rif_gruppoParlamentare ?gruppo.
    ?gruppo <http://purl.org/dc/terms/alternative> ?sigla.
    ?gruppo dc:title ?nomeGruppo.
  }}

}}
"""


def build_request_query(legislature: int) -> Dict[str, str]:
    return {"query": QUERY.format(legislature=legislature), "format": "json"}


def crawl_item(context: Context, item: dict[str, Any]) -> None:
    first_name = item.pop("nome").get("value")
    last_name = item.pop("cognome").get("value")
    dob = item.pop("dataNascita").get("value")

    entity = context.make("Person")
    entity.id = context.make_id(first_name, last_name, dob)
    h.apply_name(entity, first_name=first_name, last_name=last_name)
    entity.add("sourceUrl", item.pop("persona").get("value"))
    entity.add("birthPlace", item.pop("luogoNascita").get("value"))
    entity.add("notes", item.pop("nato").get("value"))
    entity.add("notes", item.pop("info", {}).get("value"))
    entity.add("political", item.pop("nomeGruppo").get("value"))
    entity.add("political", item.pop("sigla").get("value"))
    entity.add("gender", item.pop("genere").get("value"))
    entity.add("citizenship", "it")
    h.apply_date(entity, "birthDate", dob)
    h.apply_date(entity, "modifiedAt", item.pop("aggiornamento").get("value"))

    position = h.make_position(
        context,
        name="Member of the Chamber of Deputies",
        wikidata_id="Q18558478",
        country="it",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    start_date = item.pop("inizioMandato").get("value")
    end_date = item.pop("fineMandato", {}).get("value")
    occupancy = h.make_occupancy(
        context,
        person=entity,
        position=position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=end_date,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(position)
        context.emit(entity)

    context.audit_data(item)


def get_current_legislature(context: Context) -> int:
    # We start crawling from the 18th, which is the previous one at the time of writing.
    current_legislature = 18
    while True:
        assert current_legislature < 30, "We probably ended up in an endless loop"

        data = context.fetch_json(
            context.data_url, params=build_request_query(current_legislature + 1)
        )
        num_senators = len(data["results"]["bindings"])
        if num_senators > 0:
            context.log.info(
                f"Found {num_senators} senators in "
                f"legislature {current_legislature + 1}, trying next one."
            )
            current_legislature += 1
        else:
            context.log.info(
                f"No senators found in legislature {current_legislature + 1}, "
                f"so {current_legislature} is the current one."
            )
            break
    return current_legislature


def crawl(context: Context) -> None:
    current_legislature = get_current_legislature(context)
    last_two_legislatures = [current_legislature - 1, current_legislature]
    for leg in last_two_legislatures:
        context.log.info(f"Crawling legislature {leg}")
        data = context.fetch_json(context.data_url, params=build_request_query(leg))
        for item in data["results"]["bindings"]:
            crawl_item(context, item)
