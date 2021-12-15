import logging
from typing import Any, Dict, Optional, Set

from opensanctions.core import Context, Dataset, Entity, setup
from opensanctions import helpers as h
from opensanctions.wikidata.api import get_entity
from opensanctions.wikidata.lang import pick_obj_lang
from opensanctions.wikidata.props import IGNORE, PROPS_DIRECT, PROPS_FAMILY
from opensanctions.wikidata.claim import Claim


def apply_claim(
    context: Context, proxy: Entity, claim: Claim, depth: int, seen: Set[str]
):
    prop = PROPS_DIRECT.get(claim.property)
    if prop is not None:
        value = claim.text
        if prop == "gender":
            value = h.clean_gender(value)
        proxy.add(prop, value)
        return
    if claim.property in PROPS_FAMILY:
        if depth < 1 or claim.qid in seen:
            return
        other_data = get_entity(claim.qid)
        if other_data is None:
            return
        # TODO mark RCA
        other = entity_to_ftm(
            context, other_data, target=False, depth=depth - 1, seen=seen
        )
        if other is None:
            return
        family = context.make("Family")
        family.id = context.make_slug(claim.id)
        family.add("person", proxy.id)
        family.add("relative", other.id)
        family.add("relationship", claim.property_label)

        for qual in claim.get_qualifier("P580"):
            family.add("startDate", qual.text)

        for qual in claim.get_qualifier("P582"):
            family.add("endDate", qual.text)

        for qual in claim.get_qualifier("P1039"):
            family.add("relationship", qual.text)

        for ref in claim.references:
            # print(ref.snaks)
            for snak in ref.get("P854"):
                family.add("sourceUrl", snak.text)
        context.emit(family)
        return
    # TODO: memberships, employers?
    if claim.property in IGNORE:
        return
    # if claim.type != "external-id":
    #     context.log.warning(
    #         "Claim",
    #         prop=claim.property,
    #         prop_label=claim.property_label,
    #         type=claim.type,
    #         value_type=claim.value_type,
    #         value=claim.text,
    #     )


def entity_to_ftm(
    context: Context,
    entity: Dict[str, Any],
    schema: str = "Person",
    target: bool = True,
    depth: int = 2,
    seen: Optional[Set[str]] = None,
    **kwargs: Optional[str]
):
    if seen is None:
        seen = set()
    proxy = context.make(schema, target=target)
    proxy.id = entity.pop("id")
    seen = seen.union([proxy.id])
    proxy.add("modifiedAt", entity.pop("modified"))
    proxy.add("wikidataId", proxy.id)
    for prop, value in kwargs.items():
        proxy.add(prop, value)
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
    instance_of = [Claim(c).qid for c in claims.pop("P31", [])]
    if proxy.schema.is_a("Person") and "Q5" not in instance_of:
        context.log.error("Person is not a Q5", qid=proxy.id)
        return

    for prop_claims in claims.values():
        for claim_data in prop_claims:
            claim = Claim(claim_data)
            apply_claim(context, proxy, claim, depth, seen)

    # TODO: get back to this later:
    entity.pop("sitelinks")

    if h.check_person_cutoff(proxy):
        return

    # context.pprint(entity)
    context.emit(proxy, unique=True)
    return proxy


if __name__ == "__main__":
    setup(logging.INFO)
    context = Context(Dataset.require("everypolitician"))
    entity = get_entity("Q7747")
    proxy = entity_to_ftm(context, entity)
    # if proxy is not None:
    #     context.pprint(proxy.to_dict())
