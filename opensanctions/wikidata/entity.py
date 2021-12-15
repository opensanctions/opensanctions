import logging

from opensanctions.core import Context, Dataset, setup
from opensanctions.wikidata.api import get_entity, get_label
from opensanctions.wikidata.lang import pick_obj_lang


def entity_to_ftm(context, entity):
    proxy = context.make("Person")
    proxy.id = entity.pop("title")
    labels = entity.pop("labels")
    proxy.add("name", pick_obj_lang(labels))
    for obj in labels.values():
        proxy.add("alias", obj["value"])

    proxy.add("notes", pick_obj_lang(entity.pop("descriptions")))
    aliases = entity.pop("aliases")
    for lang in aliases.values():
        for obj in lang:
            proxy.add("alias", obj["value"])

    return proxy


if __name__ == "__main__":
    setup(logging.INFO)
    context = Context(Dataset.require("everypolitician"))
    entity = get_entity("Q42")
    proxy = entity_to_ftm(context, entity)
    context.pprint(proxy.to_dict())
