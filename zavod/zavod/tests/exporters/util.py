from zavod.context import Context
from zavod.exporters.consolidate import consolidate_entity
from zavod.store import get_store
from zavod.exporters.fragment import ViewFragment
from zavod.integration import get_dataset_linker


def harnessed_export(exporter_class, dataset, linker=None) -> None:
    context = Context(dataset)
    context.begin(clear=False)
    if linker is None:
        linker = get_dataset_linker(dataset)
    store = get_store(dataset, linker)
    store.sync()
    view = store.view(dataset)

    exporter = exporter_class(context)
    exporter.setup()
    for entity in view.entities():
        exporter.feed_unconsolidated(entity)
        fragment = ViewFragment(view, entity)
        entity = consolidate_entity(view.store.linker, entity)
        entity = fragment.get_entity(entity.id)
        exporter.feed(entity, fragment)
    exporter.finish(view)

    context.close()
    store.close()
