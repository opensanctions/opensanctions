from typing import Any, Dict, Optional, Set

from opensanctions.core import Context, Entity
from opensanctions import helpers as h
from opensanctions.wikidata.api import get_entity
from opensanctions.wikidata.lang import pick_obj_lang
from opensanctions.wikidata.props import (
    PROPS_ASSOCIATION,
    PROPS_DIRECT,
    PROPS_FAMILY,
    PROPS_QUALIFIED,
)
from opensanctions.wikidata.claim import Claim

# SEEN_PROPS = set()


def qualify_value(context: Context, value: str, claim: Claim) -> str:
    starts = set()
    for qual in claim.get_qualifier("P580"):
        starts.add(qual.text(context))

    ends = set()
    for qual in claim.get_qualifier("P582"):
        ends.add(qual.text(context))

    dates = set()
    for qual in claim.get_qualifier("P585"):
        dates.add(qual.text(context))

    return h.make_position(value, None, starts, ends, [])


def make_link(
    context, proxy, claim, depth, seen, schema, other_schema, source_prop, target_prop
):
    if depth < 1 or claim.qid in seen:
        return
    other_data = get_entity(context, claim.qid)
    if other_data is None:
        return

    props = {}
    # Hacky: is an entity is a PEP, then by definition their relatives and
    # associates are RCA (relatives and close associates).
    if "role.pep" in proxy.get("topics"):
        props["topics"] = "role.rca"

    other = entity_to_ftm(
        context,
        other_data,
        schema=other_schema,
        target=False,
        depth=depth - 1,
        seen=seen,
        **props,
    )
    if other is None:
        return
    link = context.make(schema)
    min_id, max_id = sorted((proxy.id, other.id))
    link.id = f"wd-{claim.property}-{min_id}-{max_id}"
    link.id = link.id.lower()
    link.add(source_prop, proxy.id)
    link.add(target_prop, other.id)
    rel = claim.property_label(context)
    link.add("relationship", rel)

    for qual in claim.get_qualifier("P580"):
        text = qual.text(context)
        link.add("startDate", text)

    for qual in claim.get_qualifier("P582"):
        text = qual.text(context)
        link.add("endDate", text)

    for qual in claim.get_qualifier("P585"):
        text = qual.text(context)
        link.add("date", text)

    for ref in claim.references:
        for snak in ref.get("P854"):
            text = snak.text(context)
            link.add("sourceUrl", text)
    return link


def apply_claim(
    context: Context, proxy: Entity, claim: Claim, depth: int, seen: Set[str]
):
    prop = PROPS_DIRECT.get(claim.property)
    if prop is not None:
        value = claim.text(context)
        if prop in PROPS_QUALIFIED:
            value = qualify_value(context, value, claim)
        proxy.add(prop, value)
        return
    if claim.property in PROPS_FAMILY:
        link = make_link(
            context,
            proxy,
            claim,
            depth,
            seen,
            schema="Family",
            other_schema="Person",
            source_prop="person",
            target_prop="relative",
        )
        if link is not None:
            for qual in claim.get_qualifier("P1039"):
                text = qual.text(context)
                link.set("relationship", text)
            context.emit(link)
        return
    if claim.property in PROPS_ASSOCIATION:
        link = make_link(
            context,
            proxy,
            claim,
            depth,
            seen,
            schema="Associate",
            other_schema="Person",
            source_prop="person",
            target_prop="associate",
        )
        if link is not None:
            for qual in claim.get_qualifier("P2868"):
                text = qual.text(context)
                link.set("relationship", text)
            context.emit(link)
        return
    # TODO: memberships, employers?
    # if claim.property in IGNORE:
    #     return
    # if claim.type != "external-id" and claim.property not in SEEN_PROPS:
    #     context.log.warning(
    #         "Claim",
    #         item=proxy.id,
    #         prop=claim.property,
    #         prop_label=claim.property_label,
    #         type=claim.type,
    #         value_type=claim.value_type,
    #         value=claim.text,
    #     )
    # SEEN_PROPS.add(claim.property)


def entity_to_ftm(
    context: Context,
    entity: Dict[str, Any],
    schema: str = "Person",
    target: bool = True,
    depth: int = 1,
    seen: Optional[Set[str]] = None,
    **kwargs: Optional[str],
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
    label = pick_obj_lang(labels)
    proxy.add("name", label)
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
        context.log.info("Person is not a Q5", qid=proxy.id, label=label)
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
    context.emit(proxy)
    return proxy
