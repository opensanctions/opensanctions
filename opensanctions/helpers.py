from hashlib import sha1

from normality import stringify
from memorious.operations import db


def store_entity(context, data):
    context.params["unique"] = ["id", "name", "program"]
    context.params["children"] = [
        {
            "key": "aliases",
            "inherit": {"entity_id": "id"},
            "unique": ["entity_id", "name"],
        },
        {
            "key": "addresses",
            "inherit": {"entity_id": "id"},
            "unique": [
                "entity_id", "city", "country_code", "country_name",
                "postal_code", "region", "street", "street_2", "text"
            ],
        },
        {
            "key": "identifiers",
            "inherit": {"entity_id": "id"},
            "unique": ["entity_id", "type", "number"],
        },
        {
            "key": "nationalities",
            "inherit": {"entity_id": "id"},
            "unique": ["entity_id", "country_code"],
        },
        {
            "key": "birth_dates",
            "inherit": {"entity_id": "id"},
            "unique": ["entity_id", "date"],
        },
        {
            "key": "birth_places",
            "inherit": {"entity_id": "id"},
            "unique": ["entity_id", "place"],
        },
    ]
    db(context, data)


def remove_namespace(doc, namespace):
    """Remove namespace in the passed document in place."""
    ns = u'{%s}' % namespace
    nsl = len(ns)
    for elem in doc.getiterator():
        if elem.tag.startswith(ns):
            elem.tag = elem.tag[nsl:]
