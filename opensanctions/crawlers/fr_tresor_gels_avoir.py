import json
from typing import Any, Dict
from prefixdate import parse_parts
from pantomime.types import JSON

from opensanctions.core import Context, Entity
from opensanctions import helpers as h

SCHEMATA = {
    "Personne physique": "Person",
    "Personne morale": "Organization",
    "Navire": "Vessel",
}


def apply_prop(context: Context, entity: Entity, sanction: Entity, field: str, value):
    if field == "ALIAS":
        entity.add("alias", value.pop("Alias"))
    elif field == "SEXE":
        entity.add("gender", value.pop("Sexe"))
    elif field == "PRENOM":
        entity.add("firstName", value.pop("Prenom"))
    elif field == "NATIONALITE":
        entity.add("nationality", value.pop("Pays"))
    elif field == "TITRE":
        entity.add("position", value.pop("Titre"))
    elif field == "SITE_INTERNET":
        entity.add("website", value.pop("SiteInternet"))
    elif field == "TELEPHONE":
        entity.add("phone", value.pop("Telephone"))
    elif field == "COURRIEL":
        entity.add("email", value.pop("Courriel"))
    elif field == "NUMERO_OMI":
        entity.add("imoNumber", value.pop("NumeroOMI"))
    elif field == "DATE_DE_NAISSANCE":
        date = parse_parts(value.pop("Annee"), value.pop("Mois"), value.pop("Jour"))
        entity.add("birthDate", date)
    elif field in ("ADRESSE_PM", "ADRESSE_PP"):
        address = h.make_address(
            context,
            full=value.pop("Adresse"),
            country=value.pop("Pays"),
        )
        h.apply_address(context, entity, address)
    elif field == "LIEU_DE_NAISSANCE":
        entity.add("birthPlace", value.pop("Lieu"))
        entity.add("country", value.pop("Pays"))
    elif field == "PASSEPORT":
        entity.add("passportNumber", value.pop("NumeroPasseport"))
    elif field == "IDENTIFICATION":
        comment = value.pop("Commentaire")
        content = value.pop("Identification")
        result = context.lookup("identification", comment)
        if result is None:
            context.log.warning(
                "Unknown Identification type",
                comment=comment,
                content=content,
            )
        elif result.prop is not None:
            schema = result.schema or entity.schema.name
            entity.add_cast(schema, result.prop, content)
            if result.prop == "notes":
                entity.add(result.prop, h.clean_note(comment))
    elif field == "AUTRE_IDENTITE":
        entity.add("idNumber", value.pop("NumeroCarte"))
    elif field == "REFERENCE_UE":
        sanction.add("authorityId", value.pop("ReferenceUe"))
    elif field == "REFERENCE_ONU":
        sanction.add("unscId", value.pop("ReferenceOnu"))
    elif field == "FONDEMENT_JURIDIQUE":
        sanction.add("program", value.pop("FondementJuridiqueLabel"))
        # TODO: derive target countries?
    elif field == "MOTIFS":
        motifs = value.pop("Motifs")
        sanction.add("reason", motifs)
        entity.add("notes", motifs)
    else:
        context.log.warning("Unknown field", field=field, value=value)


def crawl_entity(context: Context, data: Dict[str, Any]):
    # context.inspect(data)
    nature = data.pop("Nature")
    schema = SCHEMATA.get(nature)
    if schema is None:
        context.log.error("Unknown entity type", nature=nature)
        return
    entity = context.make(schema)
    entity.id = context.make_slug(data.pop("IdRegistre"))
    sanction = h.make_sanction(context, entity)
    for detail in data.pop("RegistreDetail"):
        field = detail.pop("TypeChamp")
        for value in detail.pop("Valeur"):
            apply_prop(context, entity, sanction, field, value)

    name = data.pop("Nom")
    h.apply_name(
        entity,
        first_name=entity.first("firstName", quiet=True),
        tail_name=name,
        quiet=True,
    )
    entity.add("topics", "sanction")
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context):
    path = context.fetch_resource("source.json", context.dataset.data.url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)

    publications = data.get("Publications")
    # date = publications.get("DatePublication")
    for detail in publications.get("PublicationDetail"):
        crawl_entity(context, detail)
