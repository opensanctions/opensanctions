from opensanctions.core import Context

from opensanctions.wikidata import get_entity, entity_to_ftm


def crawl(context: Context):
    entities = ("Q7747", "Q567")
    for qid in entities:
        data = get_entity(qid)
        entity_to_ftm(context, data)
