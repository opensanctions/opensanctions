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

from zavod.helpers.addresses import (
    apply_address,
    copy_address,
    format_address,
    make_address,
    postcode_pobox,
)
from zavod.helpers.articles import make_article, make_documentation
from zavod.helpers.change import (
    assert_dom_hash,
    assert_file_hash,
    assert_html_url_hash,
    assert_url_hash,
)
from zavod.helpers.crypto import extract_cryptos
from zavod.helpers.dates import (
    apply_date,
    apply_dates,
    backdate,
    extract_date,
    extract_years,
    parse_formats,
    replace_months,
)
from zavod.helpers.excel import (
    convert_excel_cell,
    convert_excel_date,
    parse_xls_sheet,
    parse_xlsx_sheet,
)
from zavod.helpers.html import (
    cells_to_str,
    element_text,
    element_text_hash,
    links_to_dict,
    parse_html_table,
)
from zavod.helpers.identification import make_identification
from zavod.helpers.names import (
    apply_name,
    make_name,
    needs_splitting,
    split_comma_names,
    split_names,
)
from zavod.helpers.names import split_and_apply as split_and_apply_names
from zavod.helpers.numbers import apply_number
from zavod.helpers.pdf import make_pdf_page_images, parse_pdf_table
from zavod.helpers.positions import make_occupancy, make_position
from zavod.helpers.sanctions import (
    is_active,
    lookup_sanction_program_key,
    make_sanction,
)
from zavod.helpers.securities import make_security
from zavod.helpers.text import clean_note, is_empty, multi_split, remove_bracketed
from zavod.helpers.xml import remove_namespace

__all__ = [
    "clean_note",
    "is_empty",
    "multi_split",
    "remove_bracketed",
    "make_address",
    "format_address",
    "apply_address",
    "copy_address",
    "postcode_pobox",
    "make_sanction",
    "make_article",
    "make_documentation",
    "is_active",
    "lookup_sanction_program_key",
    "make_identification",
    "extract_years",
    "parse_formats",
    "parse_xls_sheet",
    "parse_xlsx_sheet",
    "apply_date",
    "apply_dates",
    "extract_date",
    "replace_months",
    "backdate",
    "apply_number",
    "convert_excel_cell",
    "convert_excel_date",
    "make_security",
    "remove_namespace",
    "make_name",
    "apply_name",
    "make_position",
    "make_occupancy",
    "element_text",
    "element_text_hash",
    "parse_html_table",
    "cells_to_str",
    "links_to_dict",
    "extract_cryptos",
    "assert_dom_hash",
    "assert_url_hash",
    "assert_file_hash",
    "assert_html_url_hash",
    "split_comma_names",
    "needs_splitting",
    "split_and_apply_names",
    "split_names",
    "make_pdf_page_images",
    "parse_pdf_table",
]
