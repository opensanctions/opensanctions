"""Data cleaning and entity generation helpers.

This module contains a number of functions that are useful for parsing
real-world data (like XML, CSV, date formats) and converting it into
FollowTheMoney entity structures. Factory methods are provided for
handling common entity patterns as a way to reduce boilerplate code
and improve consistency across datasets.
"""
from zavod.parse.xml import remove_namespace
from zavod.parse.names import make_name, apply_name
from zavod.parse.text import clean_note, is_empty, remove_bracketed
from zavod.parse.text import multi_split
from zavod.parse.sanctions import make_sanction
from zavod.parse.addresses import make_address, apply_address, format_address
from zavod.parse.dates import extract_years, parse_date, check_no_year
from zavod.parse.dates import parse_formats
from zavod.parse.identification import make_identification
from zavod.parse.excel import convert_excel_cell

__all__ = [
    "clean_note",
    "is_empty",
    "multi_split",
    "remove_bracketed",
    "make_address",
    "format_address",
    "apply_address",
    "make_sanction",
    "make_identification",
    "extract_years",
    "parse_date",
    "parse_formats",
    "check_no_year",
    "convert_excel_cell",
    "remove_namespace",
    "make_name",
    "apply_name",
]
