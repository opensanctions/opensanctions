# Convert FtM entities into the Senzing entity format.
# cf. https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification-JSON-CSV-Mapping
#
# This format can then be used to perform record linkage against other datasets.
# As a next step, the matching results could be converted back into a
# nomenklatura resolver file and then used to generate integrated FtM entities.
#
# Senzing config note: apply the companion `senzing_config_updates.gtc` (`sz_configtool -f ...`)
# before loading. It registers what this exporter emits but a default config lacks -- the
# OPENSANCTIONS_ID and BQ_ID features, NATIONAL_ID_SUBTYPE, and the IMO_NUMBER / MMSI_NUMBER /
# CALL_SIGN / ISIN_NUMBER / OFAC_ID features. Configs older than Senzing 4.4's default also need
# `senzing_config_updates_tax_id_type.gtc` for TAX_ID_TYPE. Without them those attributes are kept
# as payload and never compared.

import re
from itertools import product
from pprint import pprint  # noqa
from typing import Any

from followthemoney import registry
from rigour.ids.wikidata import is_qid

from zavod.entity import Entity
from zavod.exporters.common import Exporter, ExportView
from zavod.runtime.urls import make_entity_url
from zavod.util import write_json

ADDR_ATTRS = ["ADDR_FULL", "PLACE_OF_BIRTH"]
STMT_PROPS_TO_MAP = {
    "imoNumber": "IMO_NUMBER",
    "mmsi": "MMSI_NUMBER",
    "callSign": "CALL_SIGN",
    "isin": "ISIN_NUMBER",
    "isinCode": "ISIN_NUMBER",
    "npiCode": "NPI_NUMBER",
    "brightQueryId": "BQ_ID",
}
NORM_TEXT = re.compile(r"[^\w\d]", re.U)
SOURCE_NAME_OVERRIDES = {
    "OS-OPENOWNERSHIP": "OPEN_OWNERSHIP",
    "OS-GLEIF": "GLEIF",
}


def push(obj: dict[str, Any], section: str, value: dict[str, Any]) -> None:
    if section not in obj:
        obj[section] = []
    for item in obj[section]:
        if item == value:
            return
    obj[section].append(value)


def map(
    entity: Entity,
    prop: str,
    obj: dict[str, Any],
    section: str,
    attr: str,
    type_attr: str | None = None,
    country: str | None = None,
    country_attr: str | None = None,
    type_val: str | None = None,
    subtype_attr: str | None = None,
    subtype_val: str | None = None,
    values: list[str] | None = None,
) -> None:
    if values is None:
        values = entity.get(prop, quiet=True)
    for value in values:
        item = {attr: value}
        if type_attr is not None:
            # The BROAD identifier scheme, which governs the exclusivity namespace.
            # Defaults to the FtM property name; pass `type_val` to place a specific
            # scheme under a broader one (e.g. ogrnCode -> TYPE=registrationNumber).
            item[type_attr] = type_val if type_val is not None else prop
        if subtype_attr is not None:
            # The FINE scheme within the broad TYPE (e.g. ogrnCode within
            # registrationNumber). A refinement, not a separate namespace: a bare
            # broad type partial-matches a subtyped one rather than conflicting. Needs
            # the ID_SUBTYPE element (see GDEV-4439 / the companion config script);
            # dropped by configs that predate it, leaving just the broad TYPE.
            item[subtype_attr] = subtype_val if subtype_val is not None else prop
        if country and country_attr is not None:
            # Qualify the identifier by country. Exclusive ids (NATIONAL_ID/TAX_ID/
            # PASSPORT) are only unique WITHIN a country, so a country puts them in
            # the right namespace and stops cross-country numbers from colliding.
            item[country_attr] = country
        push(obj, section, item)


def clean(obj: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in obj.items():
        if v is not None:
            out[k] = v
    return out


def hash_value(value: str) -> str:
    return NORM_TEXT.sub("", value).lower()


# FtM identifier properties -> (family, canonical TYPE, issuing country, NATIONAL_ID subtype).
# Tokens are the UPPER-CASE schemes of the Senzing identifier crosswalk, so every source sharing a
# Senzing instance lands in one namespace per scheme; the country is set when the scheme has only
# one issuer. Unlisted identifier properties become an OTHER_ID typed by the FtM property name.
ID_PROP_TYPES: dict[str, tuple[str, str, str | None, str | None]] = {
    "swiftBic": ("OTHER_ID", "SWIFT_BIC", None, None),
    "bikCode": ("OTHER_ID", "BIK", "ru", None),
    "okpoCode": ("OTHER_ID", "OKPO", "ru", None),
    "giiNumber": ("OTHER_ID", "GIIN", None, None),
    "permId": ("OTHER_ID", "PERMID", None, None),
    "uniqueEntityId": ("OTHER_ID", "SAM_UEI", "us", None),
    "cageCode": ("OTHER_ID", "CAGE", None, None),
    "cikCode": ("OTHER_ID", "CIK", "us", None),
    "licenseNumber": ("OTHER_ID", "LICENSE", None, None),
    "bvdId": ("OTHER_ID", "BVD_ID", None, None),
    # FtM defines serialNumber only on Airplane: the manufacturer serial number (MSN).
    "serialNumber": ("OTHER_ID", "AIRCRAFT_MSN", None, None),
    # China's Unified Social Credit Code is the company registration number.
    "uscCode": ("NATIONAL_ID", "REGISTRATION_NUMBER", "cn", "USCC"),
    "voenCode": ("TAX_ID", "VOEN", "az", None),
}
# Identifier-typed properties that do not identify one entity: a KPP reason code and a listed
# instrument's ticker/RIC are shared, an Airplane's icaoCode is its aircraft TYPE designator
# (e.g. B738), and a Vessel's crsNumber is a classification-society register number with no
# canonical scheme. Not mapped as identifiers.
NON_ID_PROPS = {"kppCode", "ticker", "ricCode", "icaoCode", "crsNumber"}

# Senzing attributes per identifier family: (number, type, country). PASSPORT and DRLIC are
# specific features with no TYPE.
ID_ATTRS: dict[str, tuple[str, str | None, str]] = {
    "NATIONAL_ID": ("NATIONAL_ID_NUMBER", "NATIONAL_ID_TYPE", "NATIONAL_ID_COUNTRY"),
    "TAX_ID": ("TAX_ID_NUMBER", "TAX_ID_TYPE", "TAX_ID_COUNTRY"),
    "OTHER_ID": ("OTHER_ID_NUMBER", "OTHER_ID_TYPE", "OTHER_ID_COUNTRY"),
    "PASSPORT": ("PASSPORT_NUMBER", None, "PASSPORT_COUNTRY"),
    "DRLIC": ("DRIVERS_LICENSE_NUMBER", None, "DRIVERS_LICENSE_STATE"),
}

# Free-text FtM `Identification.type` label -> (family, canonical TYPE, default country).
#
# An Identification's `type` is a mixed-case, punctuated string ("C.U.R.P.", "Cedula No.",
# "Driver's License No."). It decides both the Senzing feature (a driver's licence is not a
# national id, a CUIT is a tax id) and the canonical scheme within it, so the same scheme from
# another source compares equal. Keys are hash_value() of the label, so "C.U.R.P.", "CURP" and
# "c.u.r.p" collapse to one key. A family of None means the value is not an identifier at all.
# Unlisted labels fall back to classify_identification()'s keyword rules, then to an untyped
# NATIONAL_ID: Senzing still learns the id, and we never invent a token outside the vocabulary.
IDENTIFICATION_TYPES: dict[str, tuple[str | None, str | None, str | None]] = {
    hash_value(k): v
    for k, v in {
        # National-id schemes.
        "C.U.R.P.": ("NATIONAL_ID", "CURP", "mx"),
        "C.U.I.P.": ("NATIONAL_ID", "CUIP", "mx"),
        "Cedula": ("NATIONAL_ID", "CEDULA", None),
        "Cedula No.": ("NATIONAL_ID", "CEDULA", None),
        "D.N.I.": ("NATIONAL_ID", "DNI", None),
        "C.U.I.": ("NATIONAL_ID", "CUI", None),  # Guatemala
        "C.U.I.L.": ("NATIONAL_ID", "CUIL", "ar"),
        "N.I.E.": ("NATIONAL_ID", "NIE", "es"),
        "CNP (Personal Numerical Code)": ("NATIONAL_ID", "CNP", "ro"),
        "Turkish Identification Number": ("NATIONAL_ID", "TCKN", "tr"),
        "UAE Identification": ("NATIONAL_ID", "EID", "ae"),
        "Citizen's Card Number": ("NATIONAL_ID", "CCN", None),
        "Tazkira National ID Card": ("NATIONAL_ID", "TAZKIRA", "af"),
        # Company registration -> the single canonical REGISTRATION_NUMBER scheme.
        "Commercial Register": ("NATIONAL_ID", "REGISTRATION_NUMBER", None),
        "Commercial Registry Number": ("NATIONAL_ID", "REGISTRATION_NUMBER", None),
        "Registration Number": ("NATIONAL_ID", "REGISTRATION_NUMBER", None),
        "Company Number": ("NATIONAL_ID", "REGISTRATION_NUMBER", None),
        # Tax ids.
        "C.U.I.T.": ("TAX_ID", "CUIT", "ar"),
        "NIT #": ("TAX_ID", "NIT", None),
        "CNPJ": ("TAX_ID", "CNPJ", "br"),  # Brazil, legal entity
        "CPF": ("TAX_ID", "CPF", "br"),  # Brazil, natural person
        # Other documents: each its own scheme, never a national id.
        "Driver's License No.": ("DRLIC", None, None),
        "License": ("OTHER_ID", "LICENSE", None),
        "Pilot License Number": ("OTHER_ID", "LICENSE", None),
        "Birth Certificate Number": ("OTHER_ID", "BIRTH_CERT", None),
        "Residency Number": ("OTHER_ID", "RESIDENCY", None),
        "Romanian Permanent Resident": ("OTHER_ID", "RESIDENCY", "ro"),
        "Electoral Registry No.": ("OTHER_ID", "ELECTORAL", None),
        "Credencial electoral": ("OTHER_ID", "ELECTORAL", "mx"),
        "I.F.E.": ("OTHER_ID", "ELECTORAL", "mx"),
        "Cartilla de Servicio Militar Nacional": ("OTHER_ID", "MILITARY_SERVICE", "mx"),
        "Travel Document Number": ("OTHER_ID", "TRAVEL_DOC", None),
        "Refugee ID Card": ("OTHER_ID", "REFUGEE_ID", None),
        "Seafarer's Identification Document": ("OTHER_ID", "SEAFARER_ID", None),
        "Stateless Person ID Card": ("OTHER_ID", "STATELESS_ID", None),
        "VisaNumberID": ("OTHER_ID", "VISA", None),
        "Immigration No.": ("OTHER_ID", "IMMIGRATION", None),
        "Public Security and Immigration No.": ("OTHER_ID", "IMMIGRATION", None),
        # A publication reference shared by many entities, not an identifier.
        "Government Gazette Number": (None, None, None),
    }.items()
}


def classify_identification(
    raw_type: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Classify a free-text FtM Identification.type as (family, canonical TYPE, default
    country) -- see IDENTIFICATION_TYPES. Unlisted labels are typed by keyword (driver's
    licence, passport, licence, tax), otherwise an untyped NATIONAL_ID."""
    key = hash_value(raw_type or "")
    if key in IDENTIFICATION_TYPES:
        return IDENTIFICATION_TYPES[key]
    if "driv" in key:  # driver's / driving licence
        return ("DRLIC", None, None)
    if "passport" in key:
        return ("PASSPORT", None, None)
    if "licence" in key or "license" in key:
        return ("OTHER_ID", "LICENSE", None)
    if "tax" in key:
        return ("TAX_ID", None, None)
    return ("NATIONAL_ID", None, None)


def crypto_id_type(wallet: Entity) -> str:
    """CRYPTO_<asset> for a wallet address (CRYPTO_XBT, CRYPTO_ETH, ...), CRYPTO when the
    currency is unknown."""
    currency = (
        wallet.first("currency", quiet=True)
        or wallet.first("currencySymbol", quiet=True)
        or ""
    )
    currency = NORM_TEXT.sub("", currency).upper()
    return f"CRYPTO_{currency}" if currency else "CRYPTO"


def ogrn_subtype(value: str) -> str:
    """OGRN (13 digits, legal entities) vs OGRNIP (15 digits, individual entrepreneurs)."""
    return "OGRNIP" if len(NORM_TEXT.sub("", value)) == 15 else "OGRN"


def number_values(ident: dict[str, Any]) -> set[str]:
    return {v for k, v in ident.items() if k.endswith("_NUMBER")}


def is_generic_id(ident: dict[str, Any]) -> bool:
    """An untyped NATIONAL_ID / TAX_ID (from a generic idNumber / taxNumber)."""
    return any(
        f"{fam}_NUMBER" in ident and f"{fam}_TYPE" not in ident
        for fam in ("NATIONAL_ID", "TAX_ID")
    )


class SenzingExporter(Exporter):
    TITLE = "Senzing entity format"
    FILE_NAME = "senzing.json"
    MIME_TYPE = "application/json+senzing"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "wb")
        self.domain_name = "OPEN_SANCTIONS"
        source_name = f"OS_{self.dataset.name.upper()}"
        self.source_name = SOURCE_NAME_OVERRIDES.get(source_name, source_name)
        if self.dataset.is_collection and self.dataset.name != "openownership":
            self.source_name = self.domain_name

    def feed(self, entity: Entity, view: ExportView) -> None:
        if not entity.schema.matchable:
            return None

        if entity.id is None:
            return None

        record_type = None
        is_org = False

        if entity.schema.is_a("Person"):
            record_type = "PERSON"
        elif entity.schema.is_a("Organization"):
            record_type = "ORGANIZATION"
            is_org = True
        elif entity.schema.is_a("Airplane"):
            record_type = "AIRCRAFT"
            is_org = True
        elif entity.schema.is_a("Vessel"):
            record_type = "VESSEL"
            is_org = True
        elif entity.schema.is_a("Vehicle"):
            record_type = "VEHICLE"
            is_org = True
        # Skip address only records from FtM
        elif entity.schema.is_a("Address"):
            return None

        record: dict[str, Any] = {
            "DATA_SOURCE": self.source_name,
            "RECORD_ID": entity.id,
            "LAST_CHANGE": entity.last_change,
            # The canonical OpenSanctions entity id, asserted a SECOND time as a feature
            # (OPENSANCTIONS_ID, behavior A1ES) rather than only as the RECORD_ID.
            #
            # RECORD_ID is bookkeeping: Senzing never compares it, so it cannot stop two
            # different OpenSanctions entities being merged by weak evidence. A1ES DENIES on a
            # value mismatch, so once each record carries its own id, two distinct entities can
            # never co-resolve however they are pulled together.
            #
            # This matters because other sources CITE these ids. A record elsewhere carrying a
            # list of OpenSanctions ids is a REFERENCE list, not an identity; without a deny on
            # the authoritative side, every entity in such a list collapses into the citing
            # record and therefore into each other. That failure was measured on a sibling
            # corpus: one organization record carrying 700 referenced ids fused 84 distinct real
            # entities into itself until the authoritative side asserted an A1ES twin.
            #
            # Requires the companion senzing_config_updates.gtc, which registers the feature.
            "OPENSANCTIONS_ID": entity.id,
        }

        # Collect name hashes to deduplicate names that have different case but are otherwise the same
        # The hash_value() function calls a basic normalise function
        name_hashes = set()
        name_attr = "NAME_ORG" if is_org else "NAME_FULL"
        name_hashes.add(hash_value(f"{name_attr}{entity.caption}"))
        push(record, "NAMES", {"NAME_TYPE": "PRIMARY", name_attr: entity.caption})

        for name in entity.get_type_values(registry.name, matchable=True):
            if (name_hash := hash_value(f"{name_attr}{name}")) not in name_hashes:
                name_hashes.add(name_hash)
                push(record, "NAMES", {"NAME_TYPE": "ALIAS", name_attr: name})

        genders = entity.get("gender", quiet=True)
        if len(genders) == 1:
            if genders[0] == "male":
                record["GENDER"] = "M"
            if genders[0] == "female":
                record["GENDER"] = "F"

        map(entity, "topics", record, "RISKS", "TOPIC")
        map(entity, "address", record, "ADDRESSES", "ADDR_FULL")
        map(entity, "birthDate", record, "DATES", "DATE_OF_BIRTH")
        map(entity, "deathDate", record, "DATES", "DATE_OF_DEATH")
        map(entity, "incorporationDate", record, "DATES", "REGISTRATION_DATE")
        # Organization dissolution date — the org analog of a person's DATE_OF_DEATH.
        # Emitted formatted for Senzing; a consumer can register a DISSOLUTION_DATE feature
        # if they want it scored (parallel to REGISTRATION_DATE for incorporation).
        map(entity, "dissolutionDate", record, "DATES", "DISSOLUTION_DATE")
        map(entity, "birthPlace", record, "ADDRESSES", "PLACE_OF_BIRTH")
        map(
            entity,
            "country",
            record,
            "COUNTRIES",
            "COUNTRY_OF_ASSOCIATION" if is_org else "NATIONALITY",
        )
        map(entity, "nationality", record, "COUNTRIES", "NATIONALITY")
        map(entity, "citizenship", record, "COUNTRIES", "CITIZENSHIP")
        map(entity, "jurisdiction", record, "COUNTRIES", "REGISTRATION_COUNTRY")
        map(entity, "website", record, "CONTACTS", "WEBSITE_ADDRESS")
        map(entity, "email", record, "CONTACTS", "EMAIL_ADDRESS")
        map(entity, "phone", record, "CONTACTS", "PHONE_NUMBER")
        # A single best country to qualify exclusive identifiers. Registration ids use the
        # legal jurisdiction; a passport uses the holder's nationality.
        id_country = (
            entity.first("jurisdiction", quiet=True)
            or entity.first("country", quiet=True)
            or entity.first("mainCountry", quiet=True)
            or entity.first("nationality", quiet=True)
        )
        passport_country = (
            entity.first("nationality", quiet=True)
            or entity.first("citizenship", quiet=True)
            or id_country
        )
        map(
            entity,
            "passportNumber",
            record,
            "IDENTIFIERS",
            "PASSPORT_NUMBER",
            country=passport_country,
            country_attr="PASSPORT_COUNTRY",
        )
        # NATIONAL_ID / TAX_ID collapse several distinct FtM identifier schemes into one Senzing
        # field. We disambiguate with a two-level scheme + a country qualifier:
        #   *_TYPE     = the BROAD scheme, which governs the exclusivity namespace;
        #   *_SUBTYPE  = the FINE scheme within it (GDEV-4439) — a refinement, so a bare broad type
        #                partial-matches a subtyped one instead of conflicting;
        #   *_COUNTRY  = issuer (exclusive ids are only unique within a country).
        #
        # The `*_TYPE` value is the UPPER-CASE canonical scheme from the Senzing identifier crosswalk
        # (INN, VAT, REGISTRATION_NUMBER, …) rather than the raw FtM property, so every source that
        # loads into the same Senzing instance shares one exclusivity namespace per scheme. A bare
        # GENERIC default (`idNumber` / `taxNumber`) carries NO *_TYPE. Distinct schemes each get
        # their canonical *_TYPE; *_SUBTYPE is used ONLY where a broad TYPE has multiple sub-registries
        # and the source may not know which — RU has several REGISTRATION_NUMBER registries, so
        # `ogrnCode` is TYPE=REGISTRATION_NUMBER + SUBTYPE=OGRN (a bare REGISTRATION_NUMBER then
        # partial-matches it instead of conflicting).
        id_num = "NATIONAL_ID_NUMBER"
        id_type = "NATIONAL_ID_TYPE"
        id_subtype = "NATIONAL_ID_SUBTYPE"
        id_ctry = "NATIONAL_ID_COUNTRY"
        tax_num = "TAX_ID_NUMBER"
        tax_type = "TAX_ID_TYPE"
        tax_ctry = "TAX_ID_COUNTRY"
        # Generic defaults -> blank TYPE.
        map(
            entity, "idNumber", record, "IDENTIFIERS", id_num, None, id_country, id_ctry
        )
        map(
            entity,
            "taxNumber",
            record,
            "IDENTIFIERS",
            tax_num,
            None,
            id_country,
            tax_ctry,
        )
        # Distinct schemes -> canonical (upper-case) *_TYPE. An aircraft's registrationNumber
        # is its tail number, not a company registration.
        if entity.schema.is_a("Airplane"):
            map(
                entity,
                "registrationNumber",
                record,
                "IDENTIFIERS",
                "OTHER_ID_NUMBER",
                "OTHER_ID_TYPE",
                type_val="AIRCRAFT_TAIL",
            )
        else:
            map(
                entity,
                "registrationNumber",
                record,
                "IDENTIFIERS",
                id_num,
                id_type,
                id_country,
                id_ctry,
                type_val="REGISTRATION_NUMBER",
            )
        map(
            entity,
            "innCode",
            record,
            "IDENTIFIERS",
            tax_num,
            tax_type,
            id_country,
            tax_ctry,
            type_val="INN",
        )
        map(
            entity,
            "vatCode",
            record,
            "IDENTIFIERS",
            tax_num,
            tax_type,
            id_country,
            tax_ctry,
            type_val="VAT",
        )
        # ogrnCode = one of several RU registration registries -> SUBTYPE under REGISTRATION_NUMBER:
        # OGRN for a legal entity, OGRNIP for an individual entrepreneur (told apart by length).
        for ogrn in entity.get("ogrnCode", quiet=True):
            map(
                entity,
                "ogrnCode",
                record,
                "IDENTIFIERS",
                id_num,
                id_type,
                "ru",  # OGRN is a Russian registry
                id_ctry,
                type_val="REGISTRATION_NUMBER",
                subtype_attr=id_subtype,
                subtype_val=ogrn_subtype(ogrn),
                values=[ogrn],
            )
        map(entity, "socialSecurityNumber", record, "IDENTIFIERS", "SSN_NUMBER")
        map(entity, "leiCode", record, "IDENTIFIERS", "LEI_NUMBER")
        map(entity, "dunsCode", record, "IDENTIFIERS", "DUNS_NUMBER")
        map(entity, "sourceUrl", record, "SOURCE_LINKS", "SOURCE_URL")

        for _, adj in view.get_adjacent(entity):
            if adj.schema.name == "Address":
                adj_data = {"ADDR_FULL": adj.first("full")}
                push(record, "ADDRESSES", clean(adj_data))

            elif adj.schema.is_a("Identification"):
                # The document's free-text `type` decides the feature (a driver's licence is not
                # a national id, a CUIT is a tax id) and the canonical scheme within it, so the
                # same scheme bridges across sources. See IDENTIFICATION_TYPES.
                family, scheme, default_country = (
                    ("PASSPORT", None, None)
                    if adj.schema.name == "Passport"
                    else classify_identification(adj.first("type"))
                )
                if family is not None:
                    num_attr, type_attr, ctry_attr = ID_ATTRS[family]
                    adj_data = {
                        num_attr: adj.first("number"),
                        ctry_attr: adj.first("country") or default_country,
                    }
                    if type_attr is not None:
                        adj_data[type_attr] = scheme
                    push(record, "IDENTIFIERS", clean(adj_data))

            if adj.schema.edge and adj.schema.source_prop and adj.schema.target_prop:
                sources = adj.get(adj.schema.source_prop)
                targets = adj.get(adj.schema.target_prop)
                caption = adj.first("role", quiet=True) or adj.caption
                for s, t in product(sources, targets):
                    if s == entity.id:
                        edge = {
                            "REL_POINTER_ROLE": caption,
                            "REL_POINTER_DOMAIN": self.domain_name,
                            "REL_POINTER_KEY": t,
                        }
                        push(record, "RELATIONSHIPS", edge)
                    if t == entity.id:
                        edge = {
                            "REL_ANCHOR_DOMAIN": self.domain_name,
                            "REL_ANCHOR_KEY": entity.id,
                        }
                        push(record, "RELATIONSHIPS", edge)

        seen_identifiers = set()
        generic_numbers = set()
        for ident in record.get("IDENTIFIERS", []):
            seen_identifiers.update(ident.values())
            if is_generic_id(ident):
                generic_numbers.update(number_values(ident))

        for stmt in entity.get_type_statements(registry.identifier, matchable=True):
            # A specific scheme still applies when its number was only seen as a generic id;
            # the generic copy is dropped below.
            specific = stmt.prop in ID_PROP_TYPES and stmt.value in generic_numbers
            if stmt.value in seen_identifiers and not specific:
                continue
            seen_identifiers.add(stmt.value)

            if stmt.prop in NON_ID_PROPS:
                continue
            identifier_type = STMT_PROPS_TO_MAP.get(stmt.prop)
            if identifier_type is not None:
                push(record, "IDENTIFIERS", {identifier_type: stmt.value})
                continue
            if stmt.prop == "accountId":
                # A platform account on the wallet's exchange, which is its domain.
                account = {
                    "ACCOUNT_NUMBER": stmt.value,
                    "ACCOUNT_DOMAIN": entity.first("managingExchange", quiet=True),
                }
                push(record, "IDENTIFIERS", clean(account))
                continue
            if stmt.prop in ("publicKey", "cryptoAddress"):
                family, scheme = "OTHER_ID", crypto_id_type(entity)
                country, subtype = None, None
            else:
                family, scheme, country, subtype = ID_PROP_TYPES.get(
                    stmt.prop, ("OTHER_ID", stmt.prop, None, None)
                )
            num_attr, type_attr, ctry_attr = ID_ATTRS[family]
            identifier = {num_attr: stmt.value, ctry_attr: country}
            if type_attr is not None:
                identifier[type_attr] = scheme
            if subtype is not None:
                identifier["NATIONAL_ID_SUBTYPE"] = subtype
            push(record, "IDENTIFIERS", clean(identifier))

        # A generic idNumber/taxNumber often repeats the number of a typed Identification
        # document or a specific scheme (a driver's licence, a CURP, a USCC). Keep only the
        # typed copy: a bare NATIONAL_ID holding a birth-certificate number could otherwise
        # bridge to an unrelated scheme.
        if idents := record.get("IDENTIFIERS"):
            typed = {
                v for i in idents if not is_generic_id(i) for v in number_values(i)
            }
            record["IDENTIFIERS"] = [
                i for i in idents if not (is_generic_id(i) and number_values(i) & typed)
            ]

        # Retrieve the OFAC ID from the OFAC URL and add to IDENTIFIERS
        for value in entity.get("sourceUrl", quiet=True):
            if ".ofac.treas.gov/Details.aspx?id=" in value:
                _, ofac_id = value.split("?id=")
                if ofac_id:
                    push(record, "IDENTIFIERS", {"OFAC_ID": ofac_id})

        for wd_id in (entity.id, entity.first("wikidataId")):
            if wd_id is not None and is_qid(wd_id):
                wd = {
                    "TRUSTED_ID_TYPE": "WIKIDATA",
                    "TRUSTED_ID_NUMBER": wd_id,
                }
                push(record, "IDENTIFIERS", wd)

        entity_url = make_entity_url(entity)
        if entity_url is not None:
            record["URL"] = entity_url

        if entity.schema.is_a("Organization"):
            for addr in record.get("ADDRESSES", []):
                addr["ADDR_TYPE"] = "BUSINESS"

        # Collect address hashes to deduplicate addresses that have different case but are otherwise the same
        # The hash_value() function calls a basic normalise function
        if (addrs_list := record.get("ADDRESSES", [])) and len(addrs_list) > 1:
            addr_hashes = set()
            unique_addrs = []
            for addr_dict in addrs_list:
                # Try and find a value for each key seen in addresses, if can find a value hash it and keep distinct versions
                for addr_attr in ADDR_ATTRS:
                    if addr_value := addr_dict.get(addr_attr, ""):
                        # Check if it is in the set instead of just doing an add to get PRIMARY type on uniques and not have an alias replace it
                        if (
                            addr_hash := hash_value(f"{addr_attr}{addr_value}")
                        ) not in addr_hashes:
                            addr_hashes.add(addr_hash)
                            if addr_type := addr_dict.get("ADDR_TYPE", ""):
                                unique_addrs.append(
                                    {"ADDR_TYPE": addr_type, addr_attr: addr_value}
                                )
                            else:
                                unique_addrs.append({addr_attr: addr_value})

            if len(addrs_list) != len(addr_hashes):
                record["ADDRESSES"] = unique_addrs

        # Emit the recommended single-list FEATURES schema (one list for every feature) rather than
        # the legacy per-feature sub-lists (NAMES/ADDRESSES/IDENTIFIERS/...). Senzing accepts both and
        # resolves them identically; the single list is cleaner and consistent across mappers. Built
        # via sub-lists above (so the address/name dedup and BUSINESS tagging stay simple), flattened
        # here. Record-level metadata (DATA_SOURCE/RECORD_ID/LAST_CHANGE/URL) stays at the top level.
        feats: list[dict[str, Any]] = []
        if record_type is not None:
            feats.append({"RECORD_TYPE": record_type})
        if (gender := record.pop("GENDER", None)) is not None:
            feats.append({"GENDER": gender})
        for section in (
            "NAMES",
            "RISKS",
            "ADDRESSES",
            "DATES",
            "COUNTRIES",
            "CONTACTS",
            "IDENTIFIERS",
            "SOURCE_LINKS",
            "RELATIONSHIPS",
        ):
            feats.extend(record.pop(section, []))
        record["FEATURES"] = feats

        # pprint(record)
        write_json(record, self.fh)

    def finish(self, view: ExportView) -> None:
        self.fh.close()
        super().finish(view)
