from zavod import Context, Entity
from zavod.stateful.positions import categorise_many
from zavod.meta import get_multi_dataset
from zavod.store import get_store
from zavod.integration import get_dataset_linker


def analyze_position(context: Context, entity: Entity) -> None:
    """Analyze a position entity and emit the categorisation."""
    if entity.id is None:
        return
    entity_ids = set(entity.referents)
    entity_ids.add(entity.id)
    if not entity_ids:
        return
    for categorisation in categorise_many(context, entity_ids):
        proxy = context.make("Position")
        proxy.id = entity.id
        proxy.add("topics", categorisation.topics)
        if proxy.get("topics"):
            context.emit(proxy)


def crawl(context: Context) -> None:
    scope = get_multi_dataset(context.dataset.inputs)
    linker = get_dataset_linker(scope)
    store = get_store(scope, linker)
    store.sync()
    view = store.view(scope)
    position_count = 0

    for entity_idx, entity in enumerate(view.entities()):
        if entity_idx > 0 and entity_idx % 1000 == 0:
            context.log.info("Processed %s entities" % entity_idx)

        if entity.schema.is_a("Position"):
            analyze_position(context, entity)
            position_count += 1
            if position_count % 1000 == 0:
                context.log.info("Analyzed %s positions" % position_count)
                context.flush()
