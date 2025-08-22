import csv
from typing import Set, Iterable
from followthemoney import registry

from normality import squash_spaces
from zavod.entity import Entity
from zavod.logs import get_logger
from zavod.runtime.urls import make_entity_url
from zavod.exporters.common import Exporter, ExportView

COLUMNS = [
    "type",
    "caption",
    "imo",
    "risk",
    "countries",
    "flag",
    "mmsi",
    "id",
    "url",
    "datasets",
    "aliases",
    # "referents",
]

log = get_logger(__name__)


def join_cell(texts: Iterable[str], sep: str = ";") -> str:
    values: Set[str] = set()
    for value in texts:
        if value is None:
            continue
        value = value.strip().replace(sep, ",")
        if len(value) == 0:
            continue
        values.add(value)
    return sep.join(sorted(values))


class MaritimeExporter(Exporter):
    TITLE = "Maritime-centric tabular format"
    FILE_NAME = "maritime.csv"
    MIME_TYPE = "text/csv"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "w", encoding="utf-8")
        self.csv = csv.writer(self.fh, dialect=csv.unix_dialect, delimiter=",")
        self.csv.writerow(COLUMNS)
        self._count_vessels = 0
        self._count_orgs = 0

    def _get_aliases(self, entity: Entity) -> Set[str]:
        names: Set[str] = set()
        for name in entity.get_type_values(registry.name, matchable=True):
            if name == entity.caption:
                continue
            name_ = squash_spaces(name)
            if len(name_) > 0:
                names.add(name_)
        return names

    def feed(self, entity: Entity, view: ExportView) -> None:
        if "imoNumber" not in entity.schema.properties:
            return
        imos = entity.get("imoNumber")
        if len(imos) == 0:
            if not entity.schema.is_a("Vessel"):
                return
            imos.append("")
        row_type = "VESSEL" if entity.schema.is_a("Vessel") else "ORGANIZATION"
        topics = entity.get("topics", quiet=True)
        risk_topics = registry.topic.RISKS.intersection(topics)
        if row_type == "VESSEL":
            self._count_vessels += 1
        else:
            self._count_orgs += 1
        for imo in imos:
            row = [
                row_type,
                entity.caption,
                imo,
                join_cell(risk_topics),
                join_cell(entity.get_type_values(registry.country, matchable=True)),
                join_cell(entity.get("flag", quiet=True)),
                join_cell(entity.get("mmsi", quiet=True)),
                entity.id,
                make_entity_url(entity),
                join_cell(entity.datasets),
                join_cell(self._get_aliases(entity)),
                # join_cell(entity.referents),
            ]
            self.csv.writerow(row)

    def finish(self, view: ExportView) -> None:
        self.fh.close()
        super().finish(view)
        log.info(
            "Exported maritime vessels and IMO organizations",
            orgs=self._count_orgs,
            vessels=self._count_vessels,
        )
