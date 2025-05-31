from zavod.context import Context
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
        fragment = ViewFragment(view, entity)
        exporter.feed(entity, fragment)
    exporter.finish(view)

    context.close()
    store.close()
