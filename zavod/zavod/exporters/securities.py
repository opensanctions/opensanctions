"""Security-centric tabular export bridging finance identifiers and sanctions risk.

Finance and sanctions speak different languages. Markets identify things by
instrument and entity codes (ISIN, LEI, RIC, PermID, FIGI); sanctions lists
describe companies and people by name, jurisdiction and registration number.
The data vendors positioned to bridge that gap (Bloomberg and peers) will not
license their mappings for an open dataset, so this export does the connecting
work itself — emitting a flat CSV that lets a consumer look up the risk attached
to the issuers behind the securities they hold.

Who reaches for this: portfolio screeners — asset managers and similar
compliance functions checking holdings for sanctions or EO 14071 exposure — by
matching the instrument identifiers they hold against the rows here.

Scope is dual-axis. A company is included when the *company itself* is
designated, or when one of its *securities* is designated — both cases belong in
the table, because a designation can attach to either. Also included are
companies caught by broad measures such as the EO 14071 investment ban, and
public-listed companies for context.

Why company-centric (one row per issuer, identifiers inlined rather than one row
per security): it is a deliberate fallback. Even when we hold no instrument
identifiers for an in-scope company, it still gets a row — so a consumer can fall
back to name-based matching, the last resort they dislike but sometimes have no
alternative to.

Limitations: coverage is best-effort and partial. A missing identifier means we
do not hold it, not that none exists; and not every in-scope security is yet
connected to its issuer. This is a risk-relevant view of issuers, not a complete
securities universe.
"""

import csv
from collections.abc import Iterable
from normality import squash_spaces
from followthemoney import registry
from rigour.boolean import bool_text

from zavod.entity import Entity
from zavod.logs import get_logger
from zavod.runtime.urls import make_entity_url
from zavod.exporters.common import Exporter, ExportView

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
NBIM = "no_nbim_exclusions"
CONTEXT_DATASETS = set(["ru_nsd_isin", "permid", "openfigi", "research", "ext_gleif"])

log = get_logger(__name__)


def join_cell(texts: Iterable[str], sep: str = ";") -> str:
    values: set[str] = set()
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

    def _get_isins(self, entity: Entity, view: ExportView) -> set[str]:
        isins = set(entity.get("isinCode", quiet=True))
        for _, adjacent in view.get_adjacent(entity):
            if adjacent.schema.is_a("Security"):
                isins.update(adjacent.get("isin"))
        return isins

    def _get_aliases(self, entity: Entity) -> set[str]:
        names: set[str] = set()
        for name in entity.get_type_values(registry.name, matchable=True):
            name_ = squash_spaces(name)
            if len(name_) > 0:
                names.add(name_)
        return names

    def feed(self, entity: Entity, view: ExportView) -> None:
        if not entity.schema.is_a("Organization"):
            return
        topics = entity.get("topics", quiet=True)
        is_sanctioned = SANCTIONED in topics
        is_public = PUBLIC in topics
        is_eo_14071 = EO_14071 in entity.datasets
        is_nbim = NBIM in entity.datasets
        if not is_sanctioned and not is_eo_14071 and not is_nbim:
            return
        self._count_entities += 1
        leis = entity.get("leiCode", quiet=True)
        self._count_leis += len(leis)
        isins = self._get_isins(entity, view)
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
            make_entity_url(entity),
            join_cell(entity.datasets),
            join_cell(key_datasets),
            join_cell(self._get_aliases(entity)),
            join_cell(entity.referents),
        ]
        self.csv.writerow(row)

    def finish(self, view: ExportView) -> None:
        self.fh.close()
        super().finish(view)
        log.info(
            "Exported securities",
            entities=self._count_entities,
            leis=self._count_leis,
            isins=self._count_isins,
        )
