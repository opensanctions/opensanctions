from zavod import Context, Entity
from zavod.logic.pep import cached_cat_library
from zavod.meta import get_multi_dataset
from zavod.store import get_store
from zavod.integration import get_dataset_linker


def analyze_position(context: Context, entity: Entity) -> None:
    cat_lib = cached_cat_library(context)
    entity_ids = entity.referents
    if entity.id is not None:
        entity_ids.add(entity.id)
    if not entity_ids:
        return
    for entity_id in entity_ids:
        categorisation = cat_lib.get_categorisation(entity_id)
        if categorisation is None:
            continue
        proxy = context.make("Position")
        proxy.id = entity_id
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
            context.cache.flush()

        if entity.schema.is_a("Position"):
            analyze_position(context, entity)
            position_count += 1
            if position_count % 1000 == 0:
                context.log.info("Analyzed %s positions" % position_count)
                context.cache.flush()
