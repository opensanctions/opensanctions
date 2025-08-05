# Convert FtM entities into the Senzing entity format.
# cf. https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification-JSON-CSV-Mapping
#
# This format can then be used to perform record linkage against other datasets.
# As a next step, the matching results could be converted back into a
# nomenklatura resolver file and then used to generate integrated FtM entities.

import re
from itertools import product
from pprint import pprint  # noqa
from typing import Any, Dict

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


def push(obj: Dict[str, Any], section: str, value: Dict[str, Any]) -> None:
    if section not in obj:
        obj[section] = []
    for item in obj[section]:
        if item == value:
            return
    obj[section].append(value)


def map(
    entity: Entity, prop: str, obj: Dict[str, Any], section: str, attr: str
) -> None:
    for value in entity.get(prop, quiet=True):
        push(obj, section, {attr: value})


def clean(obj: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in obj.items():
        if v is not None:
            out[k] = v
    return out


def hash_value(value: str) -> str:
    return NORM_TEXT.sub("", value).lower()


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

        record: Dict[str, Any] = {
            "DATA_SOURCE": self.source_name,
            "RECORD_ID": entity.id,
            "RECORD_TYPE": record_type,
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
        map(entity, "passportNumber", record, "IDENTIFIERS", "PASSPORT_NUMBER")
        map(entity, "idNumber", record, "IDENTIFIERS", "NATIONAL_ID_NUMBER")
        map(entity, "registrationNumber", record, "IDENTIFIERS", "NATIONAL_ID_NUMBER")
        map(entity, "ogrnCode", record, "IDENTIFIERS", "NATIONAL_ID_NUMBER")
        map(entity, "taxNumber", record, "IDENTIFIERS", "TAX_ID_NUMBER")
        map(entity, "innCode", record, "IDENTIFIERS", "TAX_ID_NUMBER")
        map(entity, "vatCode", record, "IDENTIFIERS", "TAX_ID_NUMBER")
        map(entity, "socialSecurityNumber", record, "IDENTIFIERS", "SSN_NUMBER")
        map(entity, "leiCode", record, "IDENTIFIERS", "LEI_NUMBER")
        map(entity, "dunsCode", record, "IDENTIFIERS", "DUNS_NUMBER")
        map(entity, "sourceUrl", record, "SOURCE_LINKS", "SOURCE_URL")

        for _, adj in view.get_adjacent(entity):
            if adj.schema.name == "Address":
                adj_data = {"ADDR_FULL": adj.first("full")}
                push(record, "ADDRESSES", clean(adj_data))

            elif adj.schema.name == "Identification":
                adj_data = {
                    "NATIONAL_ID_NUMBER": adj.first("number"),
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

        # pprint(record)
        write_json(record, self.fh)

    def finish(self, view: ExportView) -> None:
        self.fh.close()
        super().finish(view)
