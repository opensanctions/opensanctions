from collections import defaultdict
from typing import Dict, Any, DefaultDict, Tuple
from followthemoney.types import registry

from zavod.entity import Entity
from zavod.exporters.common import Exporter
from zavod.util import write_json
from zavod.logic.pep import OccupancyStatus


PREFERENCE = {
    OccupancyStatus.CURRENT.value: 2,
    OccupancyStatus.ENDED.value: 1,
    OccupancyStatus.UNKNOWN.value: 0,
}


def observe_occupancy(
    occupancies: Dict[str, Tuple[Entity, Entity]], occupancy: Entity, position: Entity
) -> None:
    """Maintains a dict of (occupancy, position) tuples,
    keeping only the preferred occupancy for each position"""
    if position.id is None:
        raise Exception("Position id is unexpectedly None")
    seen = occupancies.get(position.id, None)
    if seen is None:
        occupancies[position.id] = (occupancy, position)
    else:
        seen_status = seen[0].get("status")[0]
        other_status = occupancy.get("status")[0]
        if PREFERENCE[other_status] > PREFERENCE[seen_status]:
            occupancies[position.id] = (occupancy, position)


class PEPSummaryExporter(Exporter):
    TITLE = "PEP position occupancy summary (Early preview)"
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
                            OccupancyStatus.CURRENT.value: 0,
                            OccupancyStatus.ENDED.value: 0,
                            OccupancyStatus.UNKNOWN.value: 0,
                        },
                    }
                ),
                "counts": {
                    "total": 0,
                    OccupancyStatus.CURRENT.value: 0,
                    OccupancyStatus.ENDED.value: 0,
                    OccupancyStatus.UNKNOWN.value: 0,
                },
            }
        )
        self.positions: DefaultDict[str, Any] = defaultdict(
            lambda: {
                "countries": defaultdict(
                    lambda: {
                        "counts": {
                            "total": 0,
                            OccupancyStatus.CURRENT.value: 0,
                            OccupancyStatus.ENDED.value: 0,
                            OccupancyStatus.UNKNOWN.value: 0,
                        }
                    }
                ),
                "counts": {
                    "total": 0,
                    OccupancyStatus.CURRENT.value: 0,
                    OccupancyStatus.ENDED.value: 0,
                    OccupancyStatus.UNKNOWN.value: 0,
                },
            }
        )

    def count_occupancy(self, occupancy: Entity, position: Entity) -> None:
        if len(position.get("name")) > 1:
            self.context.log.warn("More than one name for position.", id=position.id)
        position_name = position.get("name")[0]
        if len(occupancy.get("status")) > 1:
            self.context.log.warn("More than one status for occupancy", id=occupancy.id)
        status = occupancy.get("status")[0]
        if status not in OccupancyStatus._value2member_map_:
            self.context.log.warn(
                "Unrecognized status",
                status=status,
                position=position.id,
                occupancy=occupancy.id,
            )
            status = OccupancyStatus.UNKNOWN.value

        country_codes = position.get("country")
        for code in country_codes:
            self.countries[code]["label"] = registry.country.caption(code)
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
            occupancies: Dict[str, Tuple[Entity, Entity]] = {}
            for person_prop, person_related in self.view.get_adjacent(entity):
                if person_prop.name == "positionOccupancies":
                    for occ_prop, occ_related in self.view.get_adjacent(person_related):
                        if occ_prop.name == "post":
                            observe_occupancy(occupancies, person_related, occ_related)

            for occupancy, position in occupancies.values():
                self.count_occupancy(occupancy, position)

    def finish(self) -> None:
        output = {
            "countries": self.countries,
            "positions": self.positions,
        }
        with open(self.path, "wb") as fh:
            write_json(output, fh)
        super().finish()
