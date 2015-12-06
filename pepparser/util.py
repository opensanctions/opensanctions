from normality import slugify


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
