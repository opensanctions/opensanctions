"""
Crawler for PEP data downloaded from data.hetq.am
"""

import json
import re
from typing import Dict, List, Literal, Any
from zipfile import ZipFile

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


UNUSED_FIELDS = [
    "urlImage",
    "totalConnections",
    "totalMoney",
    "totalIncomes",
]
SupportedLanguage = Literal["en", "hy"]


def crawl_person(context: Context, zipfh: ZipFile, person_id: int):
    """Try to crawl some personal info from the HTML in
    person/{person_id}-{lang}.html"""


def crawl_list(
    context: Context,
    peps: Dict[int, Any],
    data: List[Dict[str, Any]],
    year: int,
    lang: SupportedLanguage,
):
    """Accumulate person info from yearly list.  Do not actually
    create entities yet because we want to verify some stuff."""
    for entry in data:
        personID = entry.pop("personID")
        fullName = entry.pop("fullName")
        lastPosition = entry.pop("lastPosition")
        context.audit_data(entry, UNUSED_FIELDS)
        context.log.info(
            f"Found person {personID} ({fullName}, {lastPosition}) in {year}"
        )
        if personID not in peps:
            peps[personID] = {
                f"name_{lang}": fullName,
                f"position_{lang}": lastPosition,
                "years": {year},
            }
        else:
            pep = peps[personID]
            pep["years"].add(year)
            if f"name_{lang}" not in pep:
                pep[f"name_{lang}"] = fullName
            else:
                assert pep[f"name_{lang}"] == fullName
            # NOTE: One might expect there to be different positions
            # in different years but this is not the case
            if f"position_{lang}" not in pep:
                pep[f"position_{lang}"] = lastPosition
            else:
                assert pep[f"position_{lang}"] == lastPosition


def crawl_lists(context: Context, zipfh: ZipFile):
    """Read lists of PEPs for each year covered, matching names and
    accumulating years in which the PEP was active."""
    peps = {}
    for info in zipfh.infolist():
        # zipfile.Path would be good for this but it may not work
        # correctly in all versions of Python
        m = re.match(r".*front/(\d\d\d\d)-(hy|en)\.json$", info.filename)
        if m is None:
            continue
        year_str, lang = m.groups()
        year = int(year_str)
        context.log.info(f"Reading PEPs in {lang} for {year} from {info.filename}")
        with zipfh.open(info.filename) as infh:
            data = json.load(infh)
            crawl_list(context, peps, data, year, lang)
    # Now that we have the PEPs we can grab some personal data

    # And we can grab their connections

    # And we can create entities


def crawl(context: Context):
    """Download the zip of Hetq data and create PEPs."""
    path = context.fetch_resource("hetq-data.zip", context.data_url)
    with ZipFile(path) as zipfh:
        crawl_lists(context, zipfh)
