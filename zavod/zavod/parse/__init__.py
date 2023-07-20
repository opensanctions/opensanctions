"""Data cleaning and entity generation helpers.

This module contains a number of functions that are useful for parsing
real-world data (like XML, CSV, date formats) and converting it into
FollowTheMoney entity structures. Factory methods are provided for
handling common entity patterns as a way to reduce boilerplate code
and improve consistency across datasets.
"""

from zavod.parse.addresses import make_address, format_address
from zavod.parse.xml import remove_namespace
from zavod.parse.names import make_name, apply_name
from zavod.parse.positions import make_position

__all__ = [
    "make_address",
    "format_address",
    "remove_namespace",
    "make_name",
    "apply_name",
    "make_position",
]
