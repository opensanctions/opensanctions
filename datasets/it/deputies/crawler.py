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
     ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_{legislatura}>;
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


def crawl_item(context: Context, item):
    first_name = item.pop("nome").get("value")
    last_name = item.pop("cognome").get("value")
    dob = item.pop("dataNascita").get("value")
    start_date = item.pop("inizioMandato").get("value")
    end_date = item.pop("fineMandato", {}).get("value")

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


def crawl(context: Context):
    for leg in [18, 19]:
        query = QUERY.format(legislatura=leg)
        data = context.fetch_json(
            context.data_url, params={"query": query, "format": "json"}
        )
        bindings = data["results"]["bindings"]
        for item in bindings:
            crawl_item(context, item)
