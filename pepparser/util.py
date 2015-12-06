import os
import six
from hashlib import sha1
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
        if not len(data.keys()):
            return None
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
    elif isinstance(data, six.string_types):
        if not len(data):
            return None
    return data


def obj_hash(data):
    if isinstance(data, dict):
        sum_ = sha1()
        for k, v in data.items():
            sum_.update(unicode(k).encode('utf-8'))
            sum_.update(unicode(v).encode('utf-8'))
        return sum_.hexdigest()
    return hash(data)


def unique_objs(objs):
    out = []
    if objs is None:
        return out
    seen = set()
    for obj in objs:
        sum_ = obj_hash(obj)
        if sum_ not in seen:
            seen.add(sum_)
            out.append(obj)
    return out
