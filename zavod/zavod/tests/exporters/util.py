from followthemoney.dataset import Version
from nomenklatura.resolver import Linker

from zavod.entity import Entity
from zavod.exporters.common import Exporter
from zavod.exporters.consolidate import consolidate_entity
from zavod.exporters.fragment import ViewFragment
from zavod.meta import Dataset
from zavod.runtime.statistics import Statistics
from zavod.tests.util import get_test_view, make_context


def harnessed_export(
    exporter_class: type[Exporter],
    dataset: Dataset,
    linker: Linker[Entity] | None = None,
    version: Version | None = None,
) -> None:
    """Run a single exporter over the dataset, mirroring the export loop."""
    context = make_context(dataset, version)
    context.begin()
    view = get_test_view(dataset, linker=linker, version=version)

    stats = Statistics()
    exporter = exporter_class(context, stats)
    exporter.setup()
    for entity in view.entities():
        exporter.feed_unconsolidated(entity)
        entity = consolidate_entity(view.store.linker, entity)
        fragment = ViewFragment(view, entity)
        stats.observe(entity)
        exporter.feed(entity, fragment)
    exporter.finish(view)

    context.close()
    view.store.close()
