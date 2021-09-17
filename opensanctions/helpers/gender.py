from opensanctions.helpers.lookups import types_lookup


def clean_gender(value):
    """Not clear if this function name is offensive or just weird."""
    lookup = types_lookup().get("gender")
    if lookup is None:
        return [value]
    return lookup.get_values(value, default=[value])
