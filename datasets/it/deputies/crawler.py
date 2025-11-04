from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, OccupancyStatus

# serving members of parliament: surname, first name, biographical information,
# constituency, political group, committee membership
QUERY = """
#### deputati in carica: cognome, nome, info biografiche, collegio di elezione, gruppo di appartenenza, commissione di afferenza

SELECT DISTINCT ?persona ?cognome ?nome 
?dataNascita  ?nato ?luogoNascita ?genere 
?collegio ?nomeGruppo ?sigla ?commissione ?aggiornamento  
WHERE {
?persona ocd:rif_mandatoCamera ?mandato; a foaf:Person.

## deputato
?d a ocd:deputato; ocd:aderisce ?aderisce;
ocd:rif_leg <http://dati.camera.it/ocd/legislatura.rdf/repubblica_19>;
ocd:rif_mandatoCamera ?mandato.

##anagrafica
?d foaf:surname ?cognome; foaf:gender ?genere;foaf:firstName ?nome.
OPTIONAL{
?persona <http://purl.org/vocab/bio/0.1/Birth> ?nascita.
?nascita <http://purl.org/vocab/bio/0.1/date> ?dataNascita; 
rdfs:label ?nato; ocd:rif_luogo ?luogoNascitaUri. 
?luogoNascitaUri dc:title ?luogoNascita. 
}

##aggiornamento del sistema
OPTIONAL{?d <http://lod.xdams.org/ontologies/ods/modified> ?aggiornamento.}

## mandato
?mandato ocd:rif_elezione ?elezione.  
MINUS{?mandato ocd:endDate ?fineMandato.}

## elezione
?elezione dc:coverage ?collegio.

## adesione a gruppo
OPTIONAL{
  ?aderisce ocd:rif_gruppoParlamentare ?gruppo.
  ?gruppo <http://purl.org/dc/terms/alternative> ?sigla.
  ?gruppo dc:title ?nomeGruppo.
}

MINUS{?aderisce ocd:endDate ?fineAdesione}

## organo
OPTIONAL{
?d ocd:membro ?membro.?membro ocd:rif_organo ?organo. 
?organo dc:title ?commissione .
}

MINUS{?membro ocd:endDate ?fineMembership}
}
"""


def crawl(context: Context):
    data = context.fetch_json(
        context.data_url, params={"query": QUERY, "format": "json"}
    )
    bindings = data["results"]["bindings"]
    for item in bindings:
        first_name = item.pop("nome").get("value")
        last_name = item.pop("cognome").get("value")
        dob = item.pop("dataNascita").get("value")

        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, dob)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
        entity.add("sourceUrl", item.pop("persona").get("value"))
        entity.add("birthPlace", item.pop("luogoNascita").get("value"))
        entity.add("notes", item.pop("nato").get("value"))
        entity.add("political", item.pop("nomeGruppo").get("value"))
        entity.add("political", item.pop("sigla").get("value"))
        entity.add("gender", item.pop("genere").get("value"))
        entity.add("citizenship", "it")
        h.apply_date(entity, "birthDate", dob)

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
            # Data source indicates they hold the position now
            status=OccupancyStatus.CURRENT,
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(position)
            context.emit(entity)

        context.audit_data(item, ["collegio", "commissione", "aggiornamento"])
