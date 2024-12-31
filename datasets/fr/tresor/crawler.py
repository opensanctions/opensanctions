import re
import json
from typing import Any, Dict, Optional
from normality import slugify
from prefixdate import parse_parts
from rigour.mime.types import JSON

from zavod import Context, Entity
from zavod import helpers as h

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "custom-agent",
}

SCHEMATA = {
    "Personne physique": "Person",
    "Personne morale": "Organization",
    "Navire": "Vessel",
}

SPLITS = {
    "Type d'entité": "legalForm",
    "Type d’entité": "legalForm",
    "Date d'enregistrement": "incorporationDate",
    "Lieu d'enregistrement": "address",
    "Principal établissement": "jurisdiction",
    "Établissement principal": "jurisdiction",
    "OGRN": "ogrnCode",
    "KPP": "kppCode",
    "INN": "innCode",
    "PSRN:": "registrationNumber",
    "Principal lieu d'activité": "country",
    "Numéro d'enregistrement national principal": "registrationNumber",
    "Numéro d'enregistrement national": "registrationNumber",
    "Numéro d'enregistrement": "registrationNumber",
    "Numéros d'enregistrement": "registrationNumber",
    "Numéro d'identification fiscale": "taxNumber",
    "Numéro fiscal": "taxNumber",
    "Entités associées": "*ASSOCIATES",
}


def clean_key(key: str) -> Optional[str]:
    return slugify(key)


def parse_split(context: Context, entity: Entity, full: str):
    full = full.replace("’", "'")
    splits = "|".join(SPLITS.keys())
    splits = f"({splits})"
    splits_re = re.compile(splits, re.IGNORECASE)
    parts = splits_re.split(full)
    print(parts)


def parse_identification(
    context: Context,
    entity: Entity,
    value: Dict[str, str],
):

    comment = value.pop("Commentaire")
    content = value.pop("Identification")
    full = f"{comment}: {content}".replace("::", ":").strip().strip(":").strip()
    # parse_split(context, entity, full)
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
        h.copy_address(entity, address)
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
    reg_id = data.pop("IdRegistre")

    entity_id = context.make_slug(reg_id)
    schema = SCHEMATA.get(nature)
    schema = context.lookup_value("schema_override", entity_id, schema)
    if schema is None:
        context.log.error("Unknown entity type", nature=nature)
        return
    entity = context.make(schema)
    entity.id = entity_id
    url = (
        f"https://gels-avoirs.dgtresor.gouv.fr/Gels/RegistreDetail?idRegistre={reg_id}"
    )
    entity.add("sourceUrl", url)
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
    path = context.fetch_resource("source.json", context.data_url, headers=HEADERS)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)

    publications = data.get("Publications")
    # date = publications.get("DatePublication")
    for detail in publications.get("PublicationDetail"):
        crawl_entity(context, detail)

    # print(context.get_lookup("identification").unmatched_yaml({"props": {}}))
