import json
from typing import Any, Dict
from prefixdate import parse_parts
from pantomime.types import JSON

from zavod import Context, Entity
from zavod import helpers as h

SCHEMATA = {
    "Personne physique": "Person",
    "Personne morale": "Organization",
    "Navire": "Vessel",
}


def parse_identification(
    context: Context,
    entity: Entity,
    value: Dict[str, str],
):
    comment = value.pop("Commentaire")
    content = value.pop("Identification")
    full = f"{comment}: {content}".replace("::", ":").strip().strip(":").strip()
    result = context.lookup("identification", full)
    if result is None:
        context.log.warning("Unknown identification type", identification=full)
        return
    if result.schema is not None:
        entity.add_schema(result.schema)
    if result.note:
        entity.add("notes", full, lang="fra")
    if result.props:
        for prop, value in result.props.items():
            entity.add(prop, value, lang="fra", original_value=full)
    if result.associates:
        for associate in result.associates:
            other = context.make("LegalEntity")
            other.id = context.make_slug("named", associate)
            other.add("name", associate, lang="fra")
            context.emit(other)

            link = context.make("UnknownLink")
            link.id = context.make_id(entity.id, other.id)
            link.add("subject", entity)
            link.add("object", other)
            context.emit(link)


def apply_prop(context: Context, entity: Entity, sanction: Entity, field: str, value):
    if field == "ALIAS":
        entity.add("alias", value.pop("Alias"), lang="fra")
    elif field == "SEXE":
        entity.add("gender", value.pop("Sexe"), lang="fra")
    elif field == "PRENOM":
        entity.add("firstName", value.pop("Prenom"), lang="fra")
    elif field == "NATIONALITE":
        entity.add("nationality", value.pop("Pays"), lang="fra")
    elif field == "TITRE":
        entity.add("position", value.pop("Titre"), lang="fra")
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
        entity.add("birthPlace", value.pop("Lieu"), lang="fra")
        entity.add("country", value.pop("Pays"), lang="fra")
    elif field == "PASSEPORT":
        entity.add("passportNumber", value.pop("NumeroPasseport"))
    elif field == "IDENTIFICATION":
        parse_identification(context, entity, value)
    elif field == "AUTRE_IDENTITE":
        entity.add("idNumber", value.pop("NumeroCarte"))
    elif field == "REFERENCE_UE":
        sanction.add("authorityId", value.pop("ReferenceUe"))
    elif field == "REFERENCE_ONU":
        sanction.add("unscId", value.pop("ReferenceOnu"))
    elif field == "FONDEMENT_JURIDIQUE":
        sanction.add("program", value.pop("FondementJuridiqueLabel"), lang="fra")
        # TODO: derive target countries?
    elif field == "MOTIFS":
        motifs = value.pop("Motifs")
        sanction.add("reason", motifs, lang="fra")
        entity.add("notes", motifs, lang="fra")
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


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)

    publications = data.get("Publications")
    # date = publications.get("DatePublication")
    for detail in publications.get("PublicationDetail"):
        crawl_entity(context, detail)

    # print(context.get_lookup("identification").unmatched_yaml({"props": {}}))
