from collections import defaultdict, namedtuple
from typing import Dict, List, Any, Optional, Set
from followthemoney import model
from followthemoney.types import registry

from zavod.entity import Entity
from zavod.archive import STATISTICS_FILE
from zavod.exporters.common import Exporter, ExportView
from zavod.util import write_json


def get_schema_facets(schemata: Dict[str, int]) -> List[Any]:
    facets: List[Any] = []
    for name, count in sorted(schemata.items(), key=lambda s: s[1], reverse=True):
        schema = model.get(name)
        if schema is None:
            continue
        facet = {
            "name": name,
            "count": count,
            "label": schema.label,
            "plural": schema.plural,
        }
        facets.append(facet)
    return facets


def get_country_facets(countries: Dict[str, int]) -> List[Any]:
    facets: List[Any] = []
    for code, count in sorted(countries.items(), key=lambda s: s[1], reverse=True):
        facet = {
            "code": code,
            "count": count,
            "label": registry.country.caption(code),
        }
        facets.append(facet)
    return facets


def get_sanctions_programs_facets(sanctions_programs: Dict[str, int]) -> List[Any]:
    return [
        {
            "id": program_id,
            "count": count,
        }
        for program_id, count in sanctions_programs.items()
    ]


# We don't use followthemoney.Property because we want to track manifestations of property values on specific
# schemas, even though the property might be defined somewhere higher in the type hierarchy.
SchemaProperty = namedtuple("SchemaProperty", ["schema", "property"])


def get_entities_with_prop_facets(properties: Dict[SchemaProperty, int]) -> List[Any]:
    facets: List[Any] = []
    for prop, count in sorted(properties.items()):
        facet = {
            "schema": prop.schema,
            "property": prop.property,
            "count": count,
        }
        facets.append(facet)
    return facets


class Statistics(object):
    def __init__(self) -> None:
        self.entity_count = 0
        self.last_change: Optional[str] = None
        self.schemata: Set[str] = set()
        self.qnames: Set[str] = set()

        self.thing_count = 0
        self.thing_countries: Dict[str, int] = defaultdict(int)
        self.thing_schemata: Dict[str, int] = defaultdict(int)
        self.entities_with_prop_count: Dict[SchemaProperty, int] = defaultdict(int)

        self.target_count = 0
        self.target_countries: Dict[str, int] = defaultdict(int)
        self.target_schemata: Dict[str, int] = defaultdict(int)

        self.sanctions_programs: Dict[str, int] = defaultdict(int)

    def observe(self, entity: Entity) -> None:
        self.entity_count += 1
        self.schemata.add(entity.schema.name)
        for prop in entity.iterprops():
            self.qnames.add(prop.qname)
        for prop_name, values in entity.properties.items():
            # We add 1 instead of len(values) here because we want to count the number of entities that have this
            # value set, not the number of values.
            self.entities_with_prop_count[
                SchemaProperty(entity.schema.name, prop_name)
            ] += 1

        if entity.schema.is_a("Thing"):
            self.thing_count += 1
            self.thing_schemata[entity.schema.name] += 1
            for country in entity.countries:
                self.thing_countries[country] += 1

        if entity.schema.is_a("Sanction"):
            for program_key in entity.get("programId"):
                self.sanctions_programs[program_key] += 1

        if entity.target:
            self.target_count += 1
            self.target_schemata[entity.schema.name] += 1
            for country in entity.countries:
                self.target_countries[country] += 1

        if entity.last_change is not None:
            if self.last_change is None:
                self.last_change = entity.last_change
            else:
                self.last_change = max(self.last_change, entity.last_change)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "last_change": self.last_change,
            "schemata": list(self.schemata),
            "properties": list(self.qnames),
            "entity_count": self.entity_count,
            "target_count": self.target_count,
            "targets": {
                "total": self.target_count,
                "countries": get_country_facets(self.target_countries),
                "schemata": get_schema_facets(self.target_schemata),
            },
            "sanctions": {
                "programs": get_sanctions_programs_facets(self.sanctions_programs)
            },
            "things": {
                "total": self.thing_count,
                "countries": get_country_facets(self.thing_countries),
                "schemata": get_schema_facets(self.thing_schemata),
                "entities_with_prop": get_entities_with_prop_facets(
                    self.entities_with_prop_count
                ),
            },
        }


class StatisticsExporter(Exporter):
    TITLE = "Dataset statistics"
    FILE_NAME = STATISTICS_FILE
    MIME_TYPE = "application/json"

    def setup(self) -> None:
        super().setup()
        self.stats = Statistics()

    def feed(self, entity: Entity, view: ExportView) -> None:
        self.stats.observe(entity)

    def finish(self, view: ExportView) -> None:
        with open(self.path, "wb") as fh:
            write_json(self.stats.as_dict(), fh)
        super().finish(view)
