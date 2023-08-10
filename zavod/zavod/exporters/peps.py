from collections import defaultdict
from typing import Dict, List, Any, Optional, Set
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

        country_template = lambda: {"positions": defaultdict(int)}
        self.countries: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(country_template)

    def feed(self, entity: Entity) -> None:
        if entity.schema == model.get('Person'):
            print(entity.get("name"))
            for person_prop, person_related in self.view.get_adjacent(entity):
                if person_prop.name == "positionOccupancies":
                    for occ_prop, occ_related in self.view.get_adjacent(person_related):
                        if occ_prop.name == "post":
                            country_codes = occ_related.get("country")
                            for code in country_codes:
                                position_name = occ_related.get("name")[0]
                                self.countries[code]["positions"][position_name] += 1

    def finish(self) -> None:
        output = {
            "countries": self.countries
        }
        with open(self.path, "wb") as fh:
            write_json(output, fh)
        super().finish()
