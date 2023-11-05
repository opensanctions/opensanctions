from nomenklatura.store.redis_ import RedisStore

from zavod.logs import get_logger
from zavod.meta import get_catalog
from zavod.dedupe import get_dataset_resolver
from zavod.archive import iter_dataset_statements

log = get_logger(__name__)

catalog = get_catalog()
dataset = catalog.require("sanctions")
resolver = get_dataset_resolver(dataset)
store = RedisStore(dataset, resolver, "redis://localhost:6666/0")
idx = 0
with store.writer() as writer:
    stmts = iter_dataset_statements(dataset, external=True)
    for idx, stmt in enumerate(stmts):
        if idx > 0 and idx % 100000 == 0:
            log.info(
                "Indexing aggregator...",
                statements=idx,
                scope=dataset.name,
            )
        writer.add_statement(stmt)
log.info("Local cache complete.", scope=dataset.name, statements=idx)


view = store.view(dataset, external=True)
# print(view.get_entity("Q7747").to_dict())
for idx, ent in enumerate(view.entities()):
    if idx > 0 and idx % 10000 == 0:
        log.info("Loading entities...", entities=idx, scope=dataset.name)
