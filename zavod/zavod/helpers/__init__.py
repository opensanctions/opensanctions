"""Data cleaning and entity generation helpers.

This module contains a number of functions that are useful for parsing
real-world data (like XML, CSV, date formats) and converting it into
FollowTheMoney entity structures. Factory methods are provided for
handling common entity patterns as a way to reduce boilerplate code
and improve consistency across datasets.

A typical use might look like this:

```python
from zavod import Context
from zavod import helpers as h

def crawl(context: Context) -> None:
    # ... fetch some data
    for row in data:
        entity = context.make("Person")
        entity.id = context.make_id(row.get("id"))
        # Using the helper guarantees a consistent handling of the 
        # attributes, and in this case will also automatically
        # generate a full name for the entity:
        h.apply_name(
            entity,
            first_name=row.get("first_name"),
            patronymic=row.get("patronymic"),
            last_name=row.get("last_name"),
            title=row.get("title"),
        )
        context.emit(entity)
```

Any data wrangling code that is repeated in three or more crawlers should
be considered for inclusion in the helper library.
"""

from zavod.helpers.xml import remove_namespace
from zavod.helpers.names import make_name, apply_name, split_comma_names
from zavod.helpers.positions import make_position, make_occupancy
from zavod.helpers.text import clean_note, is_empty, remove_bracketed
from zavod.helpers.text import multi_split
from zavod.helpers.sanctions import make_sanction
from zavod.helpers.addresses import make_address, apply_address, format_address
from zavod.helpers.dates import extract_years, parse_date, check_no_year
from zavod.helpers.dates import parse_formats
from zavod.helpers.identification import make_identification
from zavod.helpers.securities import make_security
from zavod.helpers.excel import convert_excel_cell, convert_excel_date
from zavod.helpers.html import parse_table

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
    "convert_excel_date",
    "make_security",
    "remove_namespace",
    "make_name",
    "apply_name",
    "make_position",
    "make_occupancy",
    "parse_table",
    "split_comma_names",
]
