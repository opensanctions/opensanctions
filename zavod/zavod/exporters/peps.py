from collections import defaultdict
from typing import Dict, Any, DefaultDict, NewType

from zavod.entity import Entity
from zavod.exporters.common import Exporter
from zavod.util import write_json
from zavod import helpers as h


CountryCode = NewType("CountryCode", str)
PositionLabel = NewType("PositionLabel", str)
PositionId = NewType("PositionId", str)

# country code -> position id -> position summary
PositionCount = Dict[str, str | int]
PositionSummary = DefaultDict[PositionId, PositionCount]
Country = Dict[str, PositionSummary | str | int]
CountryMap = DefaultDict[CountryCode, Country]

# position -> country code -> country summary
CountryCount = Dict[str, str | int]
CountrySummary = DefaultDict[CountryCode, PositionCount]
Position = Dict[str, CountrySummary | str | int]
PositionMap = DefaultDict[str, Position]


class PEPSummaryExporter(Exporter):
    TITLE = "PEP position occupancy summary"
    FILE_NAME = "pep-positions.json"
    MIME_TYPE = "application/json"

    def setup(self) -> None:
        super().setup()

        self.countries: DefaultDict[str, Any] = defaultdict(
            lambda: {
                "positions": defaultdict(
                    lambda: {
                        "position_name": "",
                        "counts": {
                            "total": 0,
                            h.PositionStatus.CURRENT.value: 0,
                            h.PositionStatus.ENDED.value: 0,
                            h.PositionStatus.UNKNOWN.value: 0,
                        },
                    }
                ),
                "counts": {
                    "total": 0,
                    h.PositionStatus.CURRENT.value: 0,
                    h.PositionStatus.ENDED.value: 0,
                    h.PositionStatus.UNKNOWN.value: 0,
                },
            }
        )
        self.positions: DefaultDict[str, Any] = defaultdict(
            lambda: {
                "countries": defaultdict(
                    lambda: {
                        "counts": {
                            "total": 0,
                            h.PositionStatus.CURRENT.value: 0,
                            h.PositionStatus.ENDED.value: 0,
                            h.PositionStatus.UNKNOWN.value: 0,
                        }
                    }
                ),
                "counts": {
                    "total": 0,
                    h.PositionStatus.CURRENT.value: 0,
                    h.PositionStatus.ENDED.value: 0,
                    h.PositionStatus.UNKNOWN.value: 0,
                },
            }
        )

    def observe_occupancy(self, occupancy: Entity, position: Entity) -> None:
        country_codes = position.get("country")
        for code in country_codes:
            position_name = position.get("name")[0]
            status = occupancy.get("status")[0]
            self.countries[code]["positions"][position.id][
                "position_name"
            ] = position_name
            self.countries[code]["positions"][position.id]["counts"]["total"] += 1
            self.countries[code]["positions"][position.id]["counts"][status] += 1
            self.countries[code]["counts"]["total"] += 1
            self.countries[code]["counts"][status] += 1

            self.positions[position_name]["countries"][code]["counts"]["total"] += 1
            self.positions[position_name]["countries"][code]["counts"][status] += 1
            self.positions[position_name]["counts"]["total"] += 1
            self.positions[position_name]["counts"][status] += 1

    def feed(self, entity: Entity) -> None:
        if entity.schema.name == "Person":
            for person_prop, person_related in self.view.get_adjacent(entity):
                if person_prop.name == "positionOccupancies":
                    for occ_prop, occ_related in self.view.get_adjacent(person_related):
                        if occ_prop.name == "post":
                            self.observe_occupancy(person_related, occ_related)

    def finish(self) -> None:
        output = {
            "countries": self.countries,
            "positions": self.positions,
        }
        with open(self.path, "wb") as fh:
            write_json(output, fh)
        super().finish()
