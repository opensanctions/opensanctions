from zavod.context import Context
from zavod.store import get_store


def harnessed_export(exporter_class, dataset) -> None:
    context = Context(dataset)
    context.begin(clear=False)
    store = get_store(dataset)
    view = store.view(dataset)

    exporter = exporter_class(context, view)
    exporter.setup()
    for entity in view.entities():
        exporter.feed(entity)
    exporter.finish()

    context.close()
    store.close()
