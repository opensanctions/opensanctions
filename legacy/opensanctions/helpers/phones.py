import re
from zavod.parse.text import multi_split

SPLITS = [",", "/", "(1)", "(2)", "(3)", "(4)", "(5)", "(6)", "(7)", "(8)"]
REMOVE = re.compile("(ex|ext|extension|fax|tel|\:|\-)", re.IGNORECASE)


def clean_phones(phones):
    out = []
    for phone in multi_split(phones, SPLITS):
        phone = REMOVE.sub("", phone)
        out.append(phone)
    return out
