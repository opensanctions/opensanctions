from zavod.context import Context
from zavod.exporters.consolidate import consolidate_entity
from zavod.runtime.statistics import Statistics
from zavod.store import get_store
from zavod.exporters.fragment import ViewFragment
from zavod.integration import get_dataset_linker


def get_test_view(dataset, linker=None, clear=False):
    if linker is None:
        linker = get_dataset_linker(dataset)
    store = get_store(dataset, linker)
    store.sync(clear=clear)
    return store.view(dataset)


def harnessed_export(exporter_class, dataset, linker=None) -> None:
    """Run a single exporter over the dataset, mirroring the export loop."""
    context = Context(dataset)
    context.begin(clear=False)
    view = get_test_view(dataset, linker=linker)

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
