# Convert FtM entities into the Senzing entity format.
# cf. https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification-JSON-CSV-Mapping
#
# This format can then be used to perform record linkage against other datasets.
# As a next step, the matching results could be converted back into a
# nomenklatura resolver file and then used to generate integrated FtM entities.
#
# Senzing config note (NATIONAL_ID_TYPE / TAX_ID_TYPE):
# This exporter emits typed identifiers so distinct FtM schemes stay in separate exclusivity
# namespaces. NATIONAL_ID_TYPE works in the default config. TAX_ID_TYPE needs an ID_TYPE element on
# the TAX_ID feature, which the Senzing 4.4 default config includes; on engines whose config predates
# that, apply the companion `senzing_config_updates.gtc` (`sz_configtool -f ...`) -- otherwise TAX_ID_TYPE
# is dropped at load and distinct tax schemes collide in one untyped namespace. (That script adds the
# ID_TYPE element to TAX_ID *and* to its comparison call, which is what makes the type actually scored.)

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
) -> None:
    for value in entity.get(prop, quiet=True):
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


# Free-text FtM `Identification.type` label -> canonical NATIONAL_ID scheme.
#
# The rest of this exporter routes the typed FtM properties (registrationNumber, innCode,
# vatCode, ogrnCode) through UPPER-CASE canonical schemes so every source sharing a Senzing
# instance lands in one exclusivity namespace per scheme. The `Identification` adjacency,
# though, carried `adj.first("type")` through RAW -- a mixed-case, punctuated FtM string
# ("C.U.R.P.", "Cedula No.", "National ID No.") -- so the same scheme from a different source
# (e.g. the Sayari mapper's canonical CURP / CEDULA) would NOT compare equal in the
# `*_TYPE` namespace, silently missing the cross-source bridge.
#
# Keys are hash_value() of the label (lower-cased, punctuation/space-stripped) so "C.U.R.P.",
# "CURP" and "c.u.r.p" all collapse to one key. Values are the SAME upper-case canonical
# schemes the Sayari mapper emits (config/sayari_codes.csv), so the two sources share one
# namespace per scheme. A label is listed ONLY when its scheme is both deducible from the
# label AND a canonical scheme in the shared vocabulary. Everything else -- generic labels
# ("National ID No.", "Identification Number") and anything unrecognized -- maps to a blank
# TYPE (Senzing still learns the untyped id; we never invent a label that isn't in the vocab).
NATIONAL_ID_TYPES = {
    hash_value(k): v
    for k, v in {
        # Person national-id schemes (canonical in the shared vocab).
        "C.U.R.P.": "CURP",  # Mexico
        "Cedula": "CEDULA",
        "Cedula No.": "CEDULA",
        "D.N.I.": "DNI",
        "C.U.I.T.": "CUIT",  # Argentina
        "C.U.I.L.": "CUIL",  # Argentina
        "C.U.I.": "CUI",  # Guatemala
        "CNPJ": "CNPJ",  # Brazil (legal entity)
        "CPF": "CPF",  # Brazil (natural person)
        # Company registration -> the single canonical REGISTRATION_NUMBER scheme.
        "Commercial Register": "REGISTRATION_NUMBER",
        "Commercial Registry Number": "REGISTRATION_NUMBER",
        "Registration Number": "REGISTRATION_NUMBER",
        "Company Number": "REGISTRATION_NUMBER",
    }.items()
}


def canonical_national_id_type(raw_type: str | None) -> str | None:
    """Map a free-text FtM Identification.type to a canonical NATIONAL_ID scheme, or
    None (blank) when the label is generic or unrecognized -- see NATIONAL_ID_TYPES."""
    if raw_type is None:
        return None
    return NATIONAL_ID_TYPES.get(hash_value(raw_type))


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
        map(entity, "idNumber", record, "IDENTIFIERS", id_num, None, id_country, id_ctry)
        map(entity, "taxNumber", record, "IDENTIFIERS", tax_num, None, id_country, tax_ctry)
        # Distinct schemes -> canonical (upper-case) *_TYPE.
        map(entity, "registrationNumber", record, "IDENTIFIERS", id_num, id_type, id_country, id_ctry,
            type_val="REGISTRATION_NUMBER")
        map(entity, "innCode", record, "IDENTIFIERS", tax_num, tax_type, id_country, tax_ctry,
            type_val="INN")
        map(entity, "vatCode", record, "IDENTIFIERS", tax_num, tax_type, id_country, tax_ctry,
            type_val="VAT")
        # ogrnCode = one of several RU registration registries -> SUBTYPE under REGISTRATION_NUMBER.
        map(
            entity, "ogrnCode", record, "IDENTIFIERS", id_num, id_type, id_country, id_ctry,
            type_val="REGISTRATION_NUMBER", subtype_attr=id_subtype, subtype_val="OGRN",
        )
        map(entity, "socialSecurityNumber", record, "IDENTIFIERS", "SSN_NUMBER")
        map(entity, "leiCode", record, "IDENTIFIERS", "LEI_NUMBER")
        map(entity, "dunsCode", record, "IDENTIFIERS", "DUNS_NUMBER")
        map(entity, "sourceUrl", record, "SOURCE_LINKS", "SOURCE_URL")

        for _, adj in view.get_adjacent(entity):
            if adj.schema.name == "Address":
                adj_data = {"ADDR_FULL": adj.first("full")}
                push(record, "ADDRESSES", clean(adj_data))

            elif adj.schema.name == "Identification":
                # Carry the document type (e.g. "C.U.R.P.", "Cedula No.") — sources record it
                # on the Identification but it was being dropped, leaving the exclusive
                # NATIONAL_ID untyped and prone to cross-scheme false conflicts. Canonicalize
                # it to the shared upper-case scheme (matching the typed properties above and
                # the Sayari mapper) so the SAME scheme bridges across sources; generic or
                # unrecognized labels canonicalize to blank rather than a raw, non-comparable
                # string. See NATIONAL_ID_TYPES.
                adj_data = {
                    "NATIONAL_ID_NUMBER": adj.first("number"),
                    "NATIONAL_ID_TYPE": canonical_national_id_type(adj.first("type")),
                    "NATIONAL_ID_COUNTRY": adj.first("country"),
                }
                push(record, "IDENTIFIERS", clean(adj_data))

            elif adj.schema.name == "Passport":
                adj_data = {
                    "PASSPORT_NUMBER": adj.first("number"),
                    "PASSPORT_COUNTRY": adj.first("country"),
                }
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
        for ident in record.get("IDENTIFIERS", []):
            seen_identifiers.update(ident.values())

        for stmt in entity.get_type_statements(registry.identifier, matchable=True):
            if stmt.value in seen_identifiers:
                continue
            seen_identifiers.add(stmt.value)

            identifier = {"OTHER_ID_TYPE": stmt.prop, "OTHER_ID_NUMBER": stmt.value}
            identifier_type = STMT_PROPS_TO_MAP.get(stmt.prop)
            if identifier_type is not None:
                identifier = {identifier_type: stmt.value}
            push(record, "IDENTIFIERS", identifier)

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

        if not is_qid(entity.id):
            ident = {"OTHER_ID_TYPE": self.domain_name, "OTHER_ID_NUMBER": entity.id}
            push(record, "IDENTIFIERS", ident)

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
