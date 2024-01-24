# Convert FtM entities into the Senzing entity format.
# cf. https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification-JSON-CSV-Mapping
#
# This format can then be used to perform record linkage against other datasets.
# As a next step, the matching results could be converted back into a
# nomenklatura resolver file and then used to generate integrated FtM entities.
from itertools import product
from typing import Dict, Any
from followthemoney.types import registry
from rigour.ids.wikidata import is_qid
from pprint import pprint  # noqa

from zavod.entity import Entity
from zavod.util import write_json
from zavod.exporters.common import Exporter
from zavod.exporters.util import public_url


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


class SenzingExporter(Exporter):
    TITLE = "Senzing entity format"
    FILE_NAME = "senzing.json"
    MIME_TYPE = "application/json+senzing"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "wb")
        self.domain_name = "OPEN_SANCTIONS"
        self.source_name = f"OS_{self.dataset.name.upper()}"
        if self.dataset.is_collection:
            self.source_name = self.domain_name

    def feed(self, entity: Entity) -> None:
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
        elif entity.schema.is_a("Vessel"):
            record_type = "VESSEL"
        elif entity.schema.is_a("Vehicle"):
            record_type = "VEHICLE"

        record: Dict[str, Any] = {
            "DATA_SOURCE": self.source_name,
            "RECORD_ID": entity.id,
            "RECORD_TYPE": record_type,
            "LAST_CHANGE": entity.last_change,
        }

        name_field = "NAME_ORG" if is_org else "NAME_FULL"
        push(record, "NAMES", {"NAME_TYPE": "PRIMARY", name_field: entity.caption})
        for name in entity.get_type_values(registry.name):
            if name != entity.caption:
                push(record, "NAMES", {"NAME_TYPE": "ALIAS", name_field: name})

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
        map(entity, "nationality", record, "COUNTRIES", "NATIONALITY")
        map(entity, "country", record, "COUNTRIES", "CITIZENSHIP")
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
        map(entity, "leiCode", record, "IDENTIFIERS", "LEI_NUMBER")
        map(entity, "dunsCode", record, "IDENTIFIERS", "DUNS_NUMBER")
        map(entity, "sourceUrl", record, "SOURCE_LINKS", "SOURCE_URL")

        for _, adj in self.view.get_adjacent(entity):
            if adj.schema.name == "Address":
                adj_data = {
                    "ADDR_FULL": adj.first("full"),
                    "ADDR_LINE1": adj.first("street"),
                    "ADDR_LINE2": adj.first("street2"),
                    "ADDR_CITY": adj.first("city"),
                    "ADDR_STATE": adj.first("state"),
                    "ADDR_COUNTRY": adj.first("country"),
                    "ADDR_POSTAL_CODE": adj.first("postalCode"),
                }
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
                    if s != entity.id and t != entity.id:
                        continue
                    edge = {
                        "REL_ANCHOR_DOMAIN": self.domain_name,
                        "REL_ANCHOR_KEY": s,
                        "REL_POINTER_ROLE": caption,
                        "REL_POINTER_DOMAIN": self.domain_name,
                        "REL_POINTER_KEY": t,
                    }
                    push(record, "RELATIONSHIPS", edge)

        seen_identifiers = set()
        for ident in record.get("IDENTIFIERS", []):
            seen_identifiers.update(ident.values())

        for stmt in entity.get_type_statements(registry.identifier):
            if stmt.value in seen_identifiers:
                continue
            ident = {"OTHER_ID_TYPE": stmt.prop, "OTHER_ID_NUMBER": stmt.value}
            push(record, "IDENTIFIERS", ident)

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

        entity_url = public_url(entity)
        if entity_url is not None:
            record["URL"] = entity_url
        if entity.schema.is_a("Organization"):
            for addr in record.get("ADDRESSES", []):
                addr["ADDR_TYPE"] = "BUSINESS"
        # pprint(record)
        write_json(record, self.fh)

    def finish(self) -> None:
        self.fh.close()
        super().finish()
