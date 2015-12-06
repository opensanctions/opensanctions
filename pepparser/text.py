
def combine_name(*parts):
    parts = [unicode(p).strip() for p in parts if p is not None]
    parts = [p for p in parts if len(p)]
    return ' '.join(parts)


def normalize_country(name):
    return name
