"""
Crawler for PEP data downloaded from data.hetq.am
"""

import json
import re
from typing import Dict, List, Literal, Any
from zipfile import ZipFile

from lxml.html import document_fromstring

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
BORN = "ծնվել"  # Often Ծնվել (title case)
IN_THE_YEAR = "թվականին"  # Often թ. (similar to Russian году / г.)
MONTHS = [  # Note: nominative
    "հունվար",
    "փետրվար",
    "մարտ",
    "ապրիլ",
    "մայիս",
    "հունիս",
    "հուլիս",
    "օգոստոս",
    "սեպտեմբեր",
    "հոկտեմբեր",
    "նոյեմբեր",
    "դեկտեմբեր",
]
HY_BIRTHDATE = re.compile(
    r"ծնվել է (\d+)\s*(?:թվականին|թվականի|թ.)(?:,?\s+(\S+)\s+(\d+)\s*[–-]\s*ին)?", re.I
)
EN_BIRTH = re.compile(
    r"born\s+(?:on\s+)?(\S+)\s+(\d+)\s*(?:,|in)\s*(\d+)(?:\s*in\s+([^\.]+))?", re.I
)


def crawl_person(
    context: Context, zipfh: ZipFile, person_id: int, data: Dict[str, Any]
):
    """Create entites for a PEP"""
    # We might have valid HTML, but we might also have an incomplete
    # page with a "Parse Error".  In any case we are really just
    # looking for date and place of birth.
    english = document_fromstring(
        zipfh.read(f"hetq-data/person/{person_id}-en.html")
    ).text_content()
    # Try to get date / place in English if possible
    m = EN_BIRTH.search(english)
    if m:
        context.log.info(f"Found English birth info for {person_id}: {m.groups()}")
    else:
        armenian = document_fromstring(
            zipfh.read(f"hetq-data/person/{person_id}-hy.html")
        ).text_content()
        # In Armenian we do date only because otherwise we'd need a
        # lemmatizer to map e.g. Երեւանում -> Երևան
        m = HY_BIRTHDATE.search(armenian)
        if m:
            context.log.info(f"Found Armenian birthdate for {person_id}: {m.groups()}")
        else:
            context.log.warning(f"No birth info found for {person_id}")


def crawl_list(
    context: Context,
    peps: Dict[int, Any],
    data: List[Dict[str, Any]],
    year: int,
    lang: SupportedLanguage,
):
    """Accumulate person info from yearly list.  Do not actually
    create entities yet because we want to track them across multiple
    years."""
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
    # Now that we have the PEPs we can grab some personal data and create entities
    for person_id, data in peps.items():
        crawl_person(context, zipfh, person_id, data)


def crawl(context: Context):
    """Download the zip of Hetq data and create PEPs."""
    path = context.fetch_resource("hetq-data.zip", context.data_url)
    with ZipFile(path) as zipfh:
        crawl_lists(context, zipfh)
