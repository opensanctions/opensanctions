from zavod.context import Context
from zavod.entity import Entity
from zavod.tests.enrich.test_local_enricher import UMBRELLA_CORP


def crawl(context: Context) -> None:
    entity = Entity.from_data(context.dataset, UMBRELLA_CORP)
    context.emit(entity)
