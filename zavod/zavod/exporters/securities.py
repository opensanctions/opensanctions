import csv
from typing import Set
from normality import collapse_spaces
from followthemoney.types import registry
from nomenklatura.util import bool_text

from zavod.entity import Entity
from zavod.exporters.common import Exporter

COLUMNS = [
    "caption",
    "lei_code",
    "isins",
    "countries",
    "sanctioned",
    "eo_14071",
    "id",
    "url",
    "datasets",
    "aliases",
]
SANCTIONED = "sanction"
EO_14071 = "ru_nsd_isin"


class SecuritiesExporter(Exporter):
    TITLE = "Security-centric tabular format"
    FILE_NAME = "securities.csv"
    MIME_TYPE = "text/csv"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "w", encoding="utf-8")
        self.csv = csv.writer(self.fh, dialect=csv.unix_dialect, delimiter=",")
        self.csv.writerow(COLUMNS)

    def _get_isins(self, entity: Entity) -> str:
        # TODO: normalize ISINs
        isins = set(entity.get("isinCode", quiet=True))
        for _, adjacent in self.view.get_adjacent(entity):
            if adjacent.schema.is_a("Security"):
                isins.update(adjacent.get("isin"))
        return "; ".join(isins)

    def _get_aliases(self, entity: Entity) -> str:
        names: Set[str] = set()
        for name in entity.get_type_values(registry.name, matchable=True):
            name = name.replace(";", ",")
            name_ = collapse_spaces(name)
            if name_ is not None:
                names.add(name_)
        return "; ".join(names)

    def feed(self, entity: Entity) -> None:
        if not entity.schema.is_a("Organization"):
            return
        is_sanctioned = SANCTIONED in entity.get("topics", quiet=True)
        is_eo_14071 = EO_14071 in entity.datasets
        if not is_sanctioned and not is_eo_14071:
            return
        row = [
            entity.caption,
            "; ".join(entity.get("leiCode", quiet=True)),
            self._get_isins(entity),
            "; ".join(entity.get_type_values(registry.country, matchable=True)),
            bool_text(is_sanctioned),
            bool_text(is_eo_14071),
            entity.id,
            f"https://www.opensanctions.org/entities/{entity.id}/",
            "; ".join(entity.datasets),
            self._get_aliases(entity),
        ]
        self.csv.writerow(row)

    def finish(self) -> None:
        self.fh.close()
        super().finish()
