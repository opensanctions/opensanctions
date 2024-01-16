# Convert FtM entities into the Senzing entity format.
# cf. https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification-JSON-CSV-Mapping
#
# This format can then be used to perform record linkage against other datasets.
# As a next step, the matching results could be converted back into a
# nomenklatura resolver file and then used to generate integrated FtM entities.
from typing import cast, Dict, Any, Generator, List, Optional, Union
from typing_extensions import TypedDict
from followthemoney.types import registry
from rigour.ids.wikidata import is_qid
from nomenklatura.store import View
from nomenklatura.dataset import DS

from zavod.entity import Entity
from zavod.exporters.common import Exporter
from zavod.util import write_json

Feature = Union[str, Dict[str, str]]


class SenzingRecord(TypedDict):
    DATA_SOURCE: str
    RECORD_ID: str
    RECORD_TYPE: Optional[str]
    FEATURES: List[Feature]


def map_feature(entity: Entity, features: List[Feature], prop: str, attr: str) -> None:
    for value in entity.get(prop, quiet=True):
        features.append({attr: value})


def senzing_adjacent_features(
    entity: Entity, view: View[DS, Entity]
) -> Generator[Feature, None, None]:
    for _, adj in view.get_adjacent(entity):
        adj_data: Optional[Dict[str, Optional[str]]] = None
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
        elif adj.schema.name == "Identification":
            adj_data = {
                "NATIONAL_ID_NUMBER": adj.first("number"),
                "NATIONAL_ID_COUNTRY": adj.first("country"),
            }
        elif adj.schema.name == "Passport":
            adj_data = {
                "PASSPORT_NUMBER": adj.first("number"),
                "PASSPORT_COUNTRY": adj.first("country"),
            }
        if adj_data is not None:
            values = {k: v for k, v in adj_data.items() if v is not None}
            if len(values):
                yield values


class SenzingExporter(Exporter):
    TITLE = "Senzing entity format"
    FILE_NAME = "senzing.json"
    MIME_TYPE = "application/json+senzing"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "wb")
        self.source_name = f"OS_{self.dataset.name.upper()}"
        if self.dataset.name in ("all", "default"):
            self.source_name = "OPENSANCTIONS"

    def feed(self, entity: Entity) -> None:
        if not entity.schema.is_a("LegalEntity"):
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

        record: SenzingRecord = {
            "DATA_SOURCE": self.source_name,
            "RECORD_ID": entity.id,
            "RECORD_TYPE": record_type,
            "FEATURES": [],
        }

        features: List[Feature] = []
        for name in entity.get_type_values(registry.name):
            name_type = "PRIMARY" if name == entity.caption else "ALIAS"
            name_field = "NAME_ORG" if is_org else "NAME_FULL"
            features.append({"NAME_TYPE": name_type, name_field: name})

        for gender in entity.get("gender", quiet=True):
            if gender == "male":
                features.append({"GENDER": "M"})
            if gender == "female":
                features.append({"GENDER": "F"})

        map_feature(entity, features, "address", "ADDR_FULL")
        map_feature(entity, features, "birthDate", "DATE_OF_BIRTH")
        map_feature(entity, features, "deathDate", "DATE_OF_DEATH")
        map_feature(entity, features, "birthPlace", "PLACE_OF_BIRTH")
        map_feature(entity, features, "nationality", "NATIONALITY")
        map_feature(entity, features, "country", "CITIZENSHIP")
        map_feature(entity, features, "incorporationDate", "REGISTRATION_DATE")
        map_feature(entity, features, "jurisdiction", "REGISTRATION_COUNTRY")
        map_feature(entity, features, "website", "WEBSITE_ADDRESS")
        map_feature(entity, features, "email", "EMAIL_ADDRESS")
        map_feature(entity, features, "phone", "PHONE_NUMBER")
        map_feature(entity, features, "passportNumber", "PASSPORT_NUMBER")
        map_feature(entity, features, "idNumber", "NATIONAL_ID_NUMBER")
        map_feature(entity, features, "registrationNumber", "NATIONAL_ID_NUMBER")
        map_feature(entity, features, "ogrnCode", "NATIONAL_ID_NUMBER")
        map_feature(entity, features, "taxNumber", "TAX_ID_NUMBER")
        map_feature(entity, features, "innCode", "TAX_ID_NUMBER")
        map_feature(entity, features, "vatCode", "TAX_ID_NUMBER")
        map_feature(entity, features, "leiCode", "LEI_NUMBER")
        map_feature(entity, features, "dunsCode", "DUNS_NUMBER")

        for adj_feature in senzing_adjacent_features(entity, self.view):
            features.append(adj_feature)

        for wd_id in (entity.id, entity.first("wikidataId")):
            if wd_id is not None and is_qid(wd_id):
                features.append(
                    {
                        "TRUSTED_ID_TYPE": "WIKIDATA",
                        "TRUSTED_ID_NUMBER": wd_id,
                    }
                )

        record["FEATURES"] = features
        write_json(cast(Dict[str, Any], record), self.fh)

    def finish(self) -> None:
        self.fh.close()
        super().finish()
