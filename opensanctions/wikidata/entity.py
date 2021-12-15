import logging

from opensanctions.core import Context, Dataset, setup
from opensanctions import helpers as h
from opensanctions.wikidata.api import get_entity
from opensanctions.wikidata.lang import pick_obj_lang
from opensanctions.wikidata.props import PROPS_DIRECT
from opensanctions.wikidata.claim import Claim


def apply_claim(context, proxy, claim):
    prop = PROPS_DIRECT.get(claim.property)
    if prop is not None:
        value = claim.text
        if prop == "gender":
            value = h.clean_gender(value)
        proxy.add(prop, value)
        return
    if claim.type != "external-id":
        context.log.warning(
            "Claim",
            prop=claim.property,
            prop_label=claim.property_label,
            type=claim.type,
            value_type=claim.value_type,
            value=claim.text,
        )


def entity_to_ftm(context, entity):
    proxy = context.make("Person")
    proxy.id = entity.pop("id")
    proxy.add("modifiedAt", entity.pop("modified"))
    labels = entity.pop("labels")
    proxy.add("name", pick_obj_lang(labels))
    for obj in labels.values():
        proxy.add("alias", obj["value"])

    proxy.add("notes", pick_obj_lang(entity.pop("descriptions")))
    aliases = entity.pop("aliases")
    for lang in aliases.values():
        for obj in lang:
            proxy.add("alias", obj["value"])

    claims = entity.pop("claims")
    for prop_claims in claims.values():
        for claim in prop_claims:
            apply_claim(context, proxy, Claim(claim))

    # TODO: get back to this later:
    entity.pop("sitelinks")

    # context.pprint(entity)
    return proxy


if __name__ == "__main__":
    setup(logging.INFO)
    context = Context(Dataset.require("everypolitician"))
    entity = get_entity("Q42")
    proxy = entity_to_ftm(context, entity)
    context.pprint(proxy.to_dict())
