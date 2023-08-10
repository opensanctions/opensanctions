from collections import defaultdict
from typing import Dict, List, Any, Optional, Set, DefaultDict
from followthemoney import model

from zavod.entity import Entity
from zavod.exporters.common import Exporter
from zavod.util import write_json


class PEPSummaryExporter(Exporter):
    TITLE = "PEP position occupancy summary"
    FILE_NAME = "pep-positions.json"
    MIME_TYPE = "application/json"

    def setup(self) -> None:
        super().setup()

        CountryCode = str
        PositionLabel = str
        PositionId = str
        PositionCount = Dict[str, str | int]
        PositionSummary = DefaultDict[PositionId, PositionCount]
        Country = Dict[str, PositionSummary]
        CountryMap = DefaultDict[CountryCode, Country]
        self.countries: CountryMap = defaultdict(
            lambda: {
                "positions": defaultdict(lambda: {
                    "position_name": None,
                    "occupancy_count": 0
                }),
                "occupancy_count": 0
            }
        )
        CountryCount = Dict[str, str | int]
        CountrySummary = DefaultDict[CountryCode, PositionCount]
        Position = Dict[str, CountrySummary]
        PositionMap = DefaultDict[str, Position]
        self.positions: PositionMap = defaultdict(
            lambda: {
                "countries": defaultdict(lambda: {
                    "occupancy_count": 0
                }),
                "occupancy_count": 0}
        )

    def observe_position(self, position):
        country_codes = position.get("country")
        for code in country_codes:
            position_name = position.get("name")[0]
            self.countries[code]["positions"][position.id]["position_name"] = position_name
            self.countries[code]["positions"][position.id]["occupancy_count"] += 1
            self.countries[code]["occupancy_count"] += 1

            self.positions[position_name]["countries"][code]["occupancy_count"] += 1
            self.positions[position_name]["occupancy_count"] += 1

    def feed(self, entity: Entity) -> None:
        if entity.schema.name == "Person":
            for person_prop, person_related in self.view.get_adjacent(entity):
                if person_prop.name == "positionOccupancies":
                    for occ_prop, occ_related in self.view.get_adjacent(person_related):
                        if occ_prop.name == "post":
                            self.observe_position(occ_related)

    def finish(self) -> None:
        output = {
            "countries": self.countries,
            "positions": self.positions,
        }
        with open(self.path, "wb") as fh:
            write_json(output, fh)
        super().finish()
