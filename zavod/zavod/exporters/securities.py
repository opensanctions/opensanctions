import csv
from typing import Set, Iterable
from normality import collapse_spaces
from followthemoney.types import registry
from nomenklatura.util import bool_text

from zavod.entity import Entity
from zavod.logs import get_logger
from zavod.exporters.common import Exporter

COLUMNS = [
    "caption",
    "lei",
    "perm_id",
    "isins",
    "ric",
    "countries",
    "sanctioned",
    "eo_14071",
    "public",
    "id",
    "url",
    "datasets",
    "risk_datasets",
    "aliases",
    "referents",
]
SANCTIONED = "sanction"
PUBLIC = "corp.public"
EO_14071 = "ru_nsd_isin"
CONTEXT_DATASETS = set(["ru_nsd_isin", "permid", "openfigi", "research", "ext_gleif"])

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


class SecuritiesExporter(Exporter):
    TITLE = "Security-centric tabular format"
    FILE_NAME = "securities.csv"
    MIME_TYPE = "text/csv"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "w", encoding="utf-8")
        self.csv = csv.writer(self.fh, dialect=csv.unix_dialect, delimiter=",")
        self.csv.writerow(COLUMNS)
        self._count_entities = 0
        self._count_isins = 0
        self._count_leis = 0

    def _get_isins(self, entity: Entity) -> Set[str]:
        # TODO: normalize ISINs
        isins = set(entity.get("isinCode", quiet=True))
        for _, adjacent in self.view.get_adjacent(entity):
            if adjacent.schema.is_a("Security"):
                isins.update(adjacent.get("isin"))
        return isins

    def _get_aliases(self, entity: Entity) -> Set[str]:
        names: Set[str] = set()
        for name in entity.get_type_values(registry.name, matchable=True):
            name_ = collapse_spaces(name)
            if name_ is not None:
                names.add(name_)
        return names

    def feed(self, entity: Entity) -> None:
        if not entity.schema.is_a("Organization"):
            return
        topics = entity.get("topics", quiet=True)
        is_sanctioned = SANCTIONED in topics
        is_public = PUBLIC in topics
        is_eo_14071 = EO_14071 in entity.datasets
        if not is_sanctioned and not is_eo_14071:
            return
        self._count_entities += 1
        leis = entity.get("leiCode", quiet=True)
        self._count_leis += len(leis)
        isins = self._get_isins(entity)
        self._count_isins += len(isins)
        key_datasets = set(entity.datasets).difference(CONTEXT_DATASETS)
        row = [
            entity.caption,
            join_cell(leis),
            join_cell(entity.get("permId", quiet=True)),
            join_cell(isins),
            join_cell(entity.get("ricCode", quiet=True)),
            join_cell(entity.get_type_values(registry.country, matchable=True)),
            bool_text(is_sanctioned),
            bool_text(is_eo_14071),
            bool_text(is_public),
            entity.id,
            f"https://www.opensanctions.org/entities/{entity.id}/",
            join_cell(entity.datasets),
            join_cell(key_datasets),
            join_cell(self._get_aliases(entity)),
            join_cell(entity.referents),
        ]
        self.csv.writerow(row)

    def finish(self) -> None:
        self.fh.close()
        super().finish()
        log.info(
            "Exported securities",
            entities=self._count_entities,
            leis=self._count_leis,
            isins=self._count_isins,
        )
