import os
from normality import slugify

DATA_FIXTURES = os.path.join(os.path.dirname(__file__), 'data')
SCHEMA_FIXTURES = os.path.join(os.path.dirname(__file__), 'schema')


def remove_namespace(doc, namespace):
    """Remove namespace in the passed document in place."""
    ns = u'{%s}' % namespace
    nsl = len(ns)
    for elem in doc.getiterator():
        if elem.tag.startswith(ns):
            elem.tag = elem.tag[nsl:]


def make_id(*parts):
    parts = [unicode(p) for p in parts if p is not None]
    parts = [slugify(p, sep='-') for p in parts if len(p)]
    return ':'.join(parts)


def clean_obj(data):
    if isinstance(data, dict):
        out = {}
        for k, v in data.items():
            v = clean_obj(v)
            if v is not None:
                out[k] = v
        return out
    elif isinstance(data, (list, tuple, set)):
        if not len(data):
            return
        return [clean_obj(o) for o in data]
    else:
        return data
