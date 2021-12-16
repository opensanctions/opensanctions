from opensanctions.core import Context

from opensanctions.wikidata import get_entity, entity_to_ftm


def crawl(context: Context):
    entities = (
        "Q7747",  # putin
        "Q567",  # merkel
        "Q123923",  # karadzic
        "Q456034",  # isabel dos santos
        "Q58217",  # lavrov
        "Q525666",  # sechin
        "Q315514",  # deripaska
        "Q1626421",  # kolomoyskiy
        "Q818077",  # steinmetz
        "Q20850503",  # prigozhin
        "Q298532",  # leyla aliyeva
        "Q4396930",  # roldugin
        "Q22686",  # trump
    )
    for qid in entities:
        data = get_entity(qid)
        entity_to_ftm(context, data)
