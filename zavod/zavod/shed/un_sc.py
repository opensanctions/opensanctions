from typing import Generator, List, Optional, Tuple

from zavod import Context, Entity
from zavod.util import ElementOrTree
from zavod import helpers as h


def get_persons(
    context: Context,
    prefix: str,
    doc: ElementOrTree,
    include_prefixes: Optional[List[str]] = None,
) -> Generator[Tuple[ElementOrTree, Entity], None, None]:
    yield from get_entities(
        context, prefix, doc, include_prefixes, "INDIVIDUAL", "Person"
    )


def get_legal_entities(
    context: Context,
    prefix: str,
    doc: ElementOrTree,
    include_prefixes: Optional[List[str]] = None,
) -> Generator[Tuple[ElementOrTree, Entity], None, None]:
    yield from get_entities(
        context, prefix, doc, include_prefixes, "ENTITY", "LegalEntity"
    )


def get_entities(
    context: Context,
    prefix: str,
    doc: ElementOrTree,
    include_prefixes: Optional[List[str]],
    tag: str,
    schema: str,
) -> Generator[Tuple[ElementOrTree, Entity], None, None]:
    for node in doc.findall(f".//{tag}"):
        perm_ref = node.findtext("./REFERENCE_NUMBER")
        if (
            include_prefixes is None
            or perm_ref is None
            or any([perm_ref.startswith(un_prefix) for un_prefix in include_prefixes])
        ):
            yield node, make_entity(context, prefix, schema, node)


def make_entity(
    context: Context, prefix: str, schema: str, node: ElementOrTree
) -> Entity:
    """Make an entity, set its ID, and add the name and sanction topic so that there is
    at least one property, making it useful and ready to emit."""
    entity = context.make(schema)
    entity.id = context.make_slug(node.findtext("./DATAID"), prefix=prefix)
    h.apply_name(
        entity,
        given_name=node.findtext("./FIRST_NAME"),
        second_name=node.findtext("./SECOND_NAME"),
        name3=node.findtext("./THIRD_NAME"),
        name4=node.findtext("./FOURTH_NAME"),
        quiet=True,
    )
    entity.add("topics", "sanction")
    return entity
