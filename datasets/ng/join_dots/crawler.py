import openpyxl
from typing import List, Optional, Set
from collections import defaultdict
from normality import slugify
from datetime import datetime
from pantomime.types import XLSX
from zavod import Context
from zavod import helpers as h



def crawl(context: Context):
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.fetch
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    references = defaultdict(list)
    raw_references: Set[str] = set()
    # duplicates = set()
    for sheet in workbook.worksheets:
        headers: Optional[List[str]] = None
        for row in sheet.rows:
            cells = [c.value for c in row]
            if headers is None:
                headers = [slugify(h, sep="_") for h in cells]
                continue
            row = dict(zip(headers, cells))
