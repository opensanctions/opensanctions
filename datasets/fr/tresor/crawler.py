import json
from typing import Any, Dict, Optional
import re

from datapatch import Lookup
from normality import slugify
from prefixdate import parse_parts

from followthemoney.types import registry
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

# Only word-character and dots
IDENTIFIER_SINGLE_VALUE_REGEX = re.compile(r"^[\w\.]+$")
SEPARATORS = "/:;-. "
# Order matters, earlier entries will be matched first
# We don't match free-text fields (currently address) because they could swallow up extra data of unmapped keys.
# For example, consider "Lieu d'enregistrement: Tehran, Iran - New Key: blabla", "New Key: blabla" would end up in the
# address field.
TEXT_KEYS = {
    "Type d'entité": "legalForm",
    "Date d'enregistrement": "incorporationDate",
    "Date de constitution": "incorporationDate",
    # address, which is a free text field we don't auto-parse because it's too easy for the value
    # to silently include a key we don't know about yet.
    "Lieu d'enregistrement": None,
    "Principal établissement": "jurisdiction",
    "Principaux établissements": "jurisdiction",
    "Établissement principal": "jurisdiction",
    "Siège principal d'exploitation": "jurisdiction",
    "Numéro d'identification fiscale (INN)": "innCode",
    "Numéro personnel d'identification fiscale (INN)": "innCode",
    "INN (numéro d'identification fiscale)": "innCode",
    "INN (NIF)": "innCode",
    "Numéro d'identification fiscale russe": "innCode",
    # Contains the : as the prefix because otherwise we also get a match for "Numéro d'identification fiscale (INN)"
    "INN:": "innCode",
    # "INN": "innCode",  <-- we can't currently add this
    "Numéro d'identification fiscale (OGRN)": "ogrnCode",
    "OGRN:": "ogrnCode",
    "KPP": "kppCode",
    "OKPO": "okpoCode",
    "PSRN": "registrationNumber",
    "SWIFT/BIC": "swiftBic",
    "SWIFT": "swiftBic",
    "PPC": None,
    "Principal lieu d'activité": "country",
    "Numéro d'enregistrement national principal": "registrationNumber",
    "Numéro d'enregistrement national": "registrationNumber",
    "Numéro de registre de commerce": "registrationNumber",
    "Numéro d'enregistrement": "registrationNumber",
    "Numéros d'enregistrement": "registrationNumber",
    "Numéro d'immatriculation": "registrationNumber",
    "numéro d'identification:": "registrationNumber",
    "Numéro d'identification de l'entreprise": "registrationNumber",
    "Numéro d'inscription au registre du commerce": "registrationNumber",
    "Numéro d'enregistrement de l'entreprise": "registrationNumber",
    "Code de crédit social unifié": "uscCode",
    "Numéro d'identification fiscale": "taxNumber",
    "Numéro d'identification fiscal": "taxNumber",
    "N ° d'identification fiscale": "taxNumber",
    "No d'identification fiscale": "taxNumber",
    "Numéro d'identification fiscale (NIF)": "taxNumber",
    "Numéro fiscal": "taxNumber",
    "NIF:": "taxNumber",
    "OMI": "imoNumber",
    "Courriel": "email",
    "Entités associées": None,
    "Entité associée": None,
    "Personnes associées": None,
}


def clean_key(key: str) -> Optional[str]:
    return slugify(key)


def parse_split(full: str):
    full = full.replace("’", "'")

    # We try to find any of the keys in TEXT_KEYS and split on them. The basic idea is to not split on separators,
    # but just find a key and keep reading until we see the next key.
    # NOTE: ideally, we would never split within a previously matched key. For example, "Numero OGRN: 1234" will
    # be split to ["Numero", "OGRN: 1234"], because the key "OGRN" is a suffix of "Numero OGRN". Doesn't happen
    # that often, so we just solve it with a lookup instead of making the code here more complex.
    # [0] is important so that if no key is found at the start of the string, it is still passed through
    indices = [0] + sorted([full.lower().find(key.lower()) for key in TEXT_KEYS.keys()])
    indices = [i for i in indices if i != -1]
    segments = [full[i:j] for i, j in zip(indices, indices[1:] + [None])]

    cleaned_segments = [s.strip(SEPARATORS) for s in segments]
    cleaned_segments = [s for s in cleaned_segments if s != ""]

    return cleaned_segments


def apply_identification_lookup(
    context: Context, lookup: Lookup, entity: Entity, segment: str
) -> bool:
    """Apply a lookup for an identification segment. Returns True if a match was found, else False."""
    result = lookup.match(segment)

    if result is None:
        return False

    if result.schema is not None:
        entity.add_schema(result.schema)
    if result.note:
        entity.add("notes", segment, lang="fra")
    if result.props:
        for prop, value in result.props.items():
            entity.add(prop, value, lang="fra", original_value=segment)
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

    return True


def identifier_value_is_single_value(context: Context, value: str) -> bool:
    if IDENTIFIER_SINGLE_VALUE_REGEX.match(value):
        return True

    country_override = context.lookup_value("type.country", value, value)
    country_clean = registry.country.clean(country_override)
    return country_clean is not None


def parse_identification(
    context: Context,
    entity: Entity,
    value: Dict[str, str],
):
    comment = value.pop("Commentaire")
    content = value.pop("Identification")
    full = f"{comment}: {content}".strip(SEPARATORS)

    # First, check if we have a lookup for the whole identification field,
    # overriding the segment splitting logic
    if apply_identification_lookup(
        context, context.get_lookup("identification_full"), entity, full
    ):
        return

    segments = parse_split(full)
    for segment in segments:
        if apply_identification_lookup(
            context, context.get_lookup("identification_segment"), entity, segment
        ):
            continue

        for key, propname in TEXT_KEYS.items():
            if segment.lower().startswith(key.lower()) and propname is not None:
                value = segment[len(key) :].strip(SEPARATORS)

                if not identifier_value_is_single_value(context, value):
                    # Likely multiple values, which we don't auto-parse.
                    # Add an override to identification_segment (or identification_full if the splitting doesn't make sense) or a type.country datapatch.
                    context.log.warning(
                        "Cannot reliably parse value.",
                        value=value,
                        segment=segment,
                        full=full,
                    )
                    break

                if propname == "kppCode":
                    entity.add_cast(
                        "Company", propname, value, lang="fra", original_value=segment
                    )
                elif propname == "incorporationDate":
                    h.apply_date(entity, propname, value)

                else:
                    entity.add(propname, value, lang="fra", original_value=segment)

                # We found what this key means, no need to try other keys.
                break
        else:
            # When processing this as part of daily issues, here are some options:
            # a) add a new key to TEXT_KEYS if it seems like it might be/become common
            # b) add a new lookup to identification_segment to map the segment
            # c) add a new lookup to identification_full if the splitting failed or you want to match the full segment
            #      for some other reason
            context.log.warning(
                f'Failed to parse identification segment: "{segment}"',
                segment=segment,
                full=full,
            )


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
        context.log.warning(
            f"Unknown field [{field}]: {value}", field=field, value=value
        )


def crawl_entity(context: Context, data: Dict[str, Any]):
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
    h.apply_name(
        entity,
        first_name=entity.first("firstName", quiet=True),
        tail_name=name,
        quiet=True,
    )
    entity.add("topics", "sanction")
    context.emit(entity)
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

    # context.debug_lookups()
    # print(context.get_lookup("identification").unmatched_yaml({"props": {}}))
