from typing import List, Optional
from zavod import Context, Entity
from zavod.util import ElementOrTree


def get_persons(
    context: Context,
    prefix: str,
    doc: ElementOrTree,
    committee_id_prefixes: Optional[List[str]] = None,
):
    for entity in get_entities(
        context, prefix, doc, committee_id_prefixes, "INDIVIDUAL", "Person"
    ):
        yield entity


def get_legal_entities(
    context: Context,
    prefix: str,
    doc: ElementOrTree,
    committee_id_prefixes: Optional[List[str]] = None,
):
    for entity in get_entities(
        context, prefix, doc, committee_id_prefixes, "ENTITY", "LegalEntity"
    ):
        yield entity


def get_entities(
    context: Context,
    prefix,
    doc: ElementOrTree,
    committee_id_prefixes: List[str],
    tag,
    schema,
):
    for node in doc.findall(f".//{tag}"):
        dataid = node.findtext("./DATAID")
        perm_ref = node.findtext("./REFERENCE_NUMBER")
        if committee_id_prefixes is None or any(
            [perm_ref.startswith(un_prefix) for un_prefix in committee_id_prefixes]
        ):
            yield node, make_entity(context, prefix, schema, dataid)


def make_entity(context: Context, prefix: str, schema: str, dataid: str) -> Entity:
    """Make an entity and set its ID"""
    entity = context.make(schema)
    entity.id = context.make_slug(dataid, prefix=prefix)
    return entity
