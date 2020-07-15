import logging
import countrynames
from normality import stringify

log = logging.getLogger(__name__)


def normalize_country(name):
    return countrynames.to_code(name)


def jointext(*parts, sep=" "):
    parts = [stringify(p) for p in parts]
    parts = [p for p in parts if p is not None]
    return sep.join(parts)
