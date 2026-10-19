import json
from typing import Any

from banal import ensure_list
from datapatch import Lookup
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

SEPARATORS = "/:;-. "


def apply_identification_lookup(
    context: Context, lookup: Lookup, entity: Entity, full: str
) -> bool:
    """Apply the lookup option matching an identification field. Returns True if a match was found, else False."""
    result = lookup.match(full)

    if result is None:
        return False

    if result.schema is not None:
        entity.add_schema(result.schema)
    if result.note:
        entity.add("notes", full, lang="fra")
    if result.props:
        for prop, value in result.props.items():
            if prop == "kppCode":
                entity.add_cast("Company", prop, value, lang="fra", original_value=full)
            else:
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
    if result.wallets:
        for wallet_data in result.wallets:
            currency = wallet_data.get("currency")
            publicKey = wallet_data.get("publicKey")
            if currency and publicKey:
                wallet = context.make("CryptoWallet")
                wallet.id = context.make_slug("wallet", currency, publicKey)
                wallet.add("currency", currency)
                wallet.add("publicKey", publicKey)
                wallet.add("holder", entity.id)
                context.emit(wallet)
    if result.vessels:
        for vessel_data in result.vessels:
            imo_number = vessel_data.get("imoNumber", None)
            name = vessel_data.get("name")
            vessel = context.make("Vessel")
            vessel.id = context.make_slug("vessel", imo_number, name)
            vessel.add("imoNumber", imo_number)
            vessel.add("name", name)
            vessel_rel = context.make("UnknownLink")
            vessel_rel.id = context.make_id(entity.id, vessel.id)
            vessel_rel.add("subject", entity)
            vessel_rel.add("object", vessel)
            context.emit(vessel_rel)
            context.emit(vessel)

    return True


def parse_identification(
    context: Context,
    entity: Entity,
    value: dict[str, str],
) -> None:
    """Parse the identification field and add the appropriate properties to the entity.

    The field is free text following a loose "<label>: <value>" convention. Each
    distinct field is mapped by an option in the `identification_full` lookup, whose
    leading comment holds the rules for adding one. An unmatched field raises a
    warning with the string to match on.
    """
    comment = value.pop("Commentaire")
    content = value.pop("Identification")
    full = f"{comment}: {content}".strip(SEPARATORS)

    if not apply_identification_lookup(
        context, context.get_lookup("identification_full"), entity, full
    ):
        context.log.warning("Cannot parse identification field.", full=full)


def apply_prop(
    context: Context,
    entity: Entity,
    sanction: Entity,
    field: str,
    value: dict[str, Any],
) -> None:
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
        full_address = value.pop("Adresse")
        country = value.pop("Pays")
        if ";" in full_address:
            res = context.lookup("addresses", full_address, warn_unmatched=True)
            # Sometimes the lookup returns a single string, not a list.
            addresses = ensure_list(res.addresses) if res else [full_address]
            assert isinstance(addresses, list), full_address
        else:
            addresses = [full_address]

        for full_address in addresses:
            address = h.make_address(context, full=full_address, country=country)
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
        context.log.warning(
            f"Unknown field [{field}]: {value}", field=field, value=value
        )


def crawl_entity(context: Context, data: dict[str, Any]) -> None:
    # context.inspect(data)
    nature = data.pop("Nature")
    reg_id = data.pop("IdRegistre")

    entity_id = context.make_slug(reg_id)
    schema = SCHEMATA.get(nature)
    schema = context.lookup_value("schema_override", entity_id, schema)
    if schema is None:
        context.log.error(f"Unknown entity type: {nature}", nature=nature)
        return
    entity = context.make(schema)
    entity.id = entity_id
    url = (
        f"https://gels-avoirs.dgtresor.gouv.fr/Gels/RegistreDetail?idRegistre={reg_id}"
    )
    entity.add("sourceUrl", url)
    # Extract all program names from the 'FONDEMENT_JURIDIQUE' sections.
    program_names = []
    for detail in data.get("RegistreDetail", []):
        if detail.get("TypeChamp") == "FONDEMENT_JURIDIQUE":
            for val in detail.get("Valeur", []):
                program_name = val.get("FondementJuridiqueLabel")
                if program_name:
                    program_names.append(program_name)
    # For each program name, create a separate sanction.
    for program_name in program_names:
        sanction = h.make_sanction(
            context,
            entity,
            program_name=program_name,
            source_program_key=program_name,
            program_key=h.lookup_sanction_program_key(context, program_name),
        )
    for detail in data.pop("RegistreDetail"):
        field = detail.pop("TypeChamp")
        for value in detail.pop("Valeur"):
            apply_prop(context, entity, sanction, field, value)

    name = data.pop("Nom")
    first_name = entity.first("firstName", quiet=True)
    # Entities that don't have a first name usually have their full name in the "Nom" field.
    if first_name is None:
        h.apply_name(
            entity,
            full=name,
            quiet=True,
        )
    else:
        h.apply_name(
            entity,
            first_name=first_name,
            tail_name=name,
            quiet=True,
        )
    entity.add("topics", "sanction")
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.json", context.data_url, headers=HEADERS)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path) as fh:
        data = json.load(fh)

    publications = data.get("Publications")
    # date = publications.get("DatePublication")
    for detail in publications.get("PublicationDetail"):
        crawl_entity(context, detail)

    # context.debug_lookups()
    # print(context.get_lookup("identification").unmatched_yaml({"props": {}}))
