"""
Crawler for PEP data downloaded from data.hetq.am
"""

import json
import re
from typing import Dict, List, Literal, Any, Tuple, Union
from zipfile import ZipFile

from lxml.html import document_fromstring

from zavod import Context, Entity
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
# Note: these are in the dative in the text, but that is just a suffix
# -ի so we do prefix matching.
MONTHS = [
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
DATE_FORMATS = [
    "%B %d %Y",
    "%Y-%m-%d",
]
HY_BIRTHDATE = re.compile(
    r"ծնվել է (\d+)\s*(?:թվականին|թվականի|թ.)(?:,?\s+(\S+)\s+(\d+)\s*[–-]\s*ին)?", re.I
)
EN_BIRTH = re.compile(
    r"born\s+(?:on\s+)?(\S+)\s+(\d+)\s*(?:,|in)\s*(\d+)(?:\s*in\s+([^\.]+))?", re.I
)


def get_birth_info(
    context: Context, zipfh: ZipFile, person_id: int
) -> Tuple[Union[None, str], Union[None, str]]:
    """Get birth date and place from person page."""
    # We might have valid HTML, but we might also have an incomplete
    # page with a "Parse Error".  In any case we are really just
    # looking for date and place of birth, so use the text.
    english = document_fromstring(
        zipfh.read(f"hetq-data/person/{person_id}-en.html")
    ).text_content()
    # Try to get date / place in English if possible
    birth_date = birth_place = None
    m = EN_BIRTH.search(english)
    if m:
        context.log.debug(f"Found English birth info for {person_id}: {m.groups()}")
        month, day, year, birth_place = m.groups()
        birth_date = " ".join((month, day, year))
    else:
        armenian = document_fromstring(
            zipfh.read(f"hetq-data/person/{person_id}-hy.html")
        ).text_content()
        # In Armenian we don't use the place of birth because it is in
        # locative case and we don't have a lemmatizer.
        m = HY_BIRTHDATE.search(armenian)
        if m:
            context.log.debug(f"Found Armenian birthdate for {person_id}: {m.groups()}")
            year, month, day = m.groups()
            birth_date = year
            if month is not None:
                for idx, m in enumerate(MONTHS):
                    if month.startswith(m):
                        birth_date = f"{year}-{idx + 1}-{day}"
                        break
    return birth_date, birth_place


def add_armenian_name(
    context: Context, person: Entity, name: str, lang: SupportedLanguage
):
    """Parse and add a name to a person."""
    parts = name.split()
    # Armenian names are FAMILY GIVEN (PATRONYMIC) but there might be multiple given names
    if len(parts) < 3 or len(parts) > 6:
        context.log.warning(
            f"Name {name} has {len(parts)} parts, don't know what to do"
        )
        h.apply_name(person, name, lang=lang)
        return
    kwargs = {
        "last_name": parts[0],
        "patronymic": parts[-1],
        "lang": lang,
    }
    for i in range(1, len(parts) - 1):
        kwargs[f"name{i}"] = parts[i]
    h.apply_name(person, **kwargs)


def crawl_person(
    context: Context, zipfh: ZipFile, person_id: int, data: Dict[str, Any]
):
    """Create entites for a PEP"""
    birth_date, birth_place = get_birth_info(context, zipfh, person_id)
    name_en = data.get("name_en", "").strip()
    name_hy = data.get("name_hy", "").strip()
    if name_en == "" and name_hy == "":
        context.log.warning(f"Skipping person {person_id} with no name")
        return
    person = context.make("Person")
    person.id = context.make_slug(str(person_id))
    if name_en:
        add_armenian_name(context, person, name_en, "en")
    if name_hy:
        add_armenian_name(context, person, name_hy, "hy")
    if birth_date is not None:
        person.add("birthDate", h.parse_date(birth_date, DATE_FORMATS))
    if birth_place is not None:
        person.add("birthPlace", birth_place)
    position_en = data.get("position_en", "").strip()
    position_hy = data.get("position_hy", "").strip()
    position_name = position_en or position_hy
    context.log.debug(f"position: {position_name} years: {data['years']}")
    if position_name:
        position = h.make_position(
            context,
            name=position_name,
            country="am",
        )
        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            context.log.warning(f"Person {person_id} is not PEP")
            return
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=str(min(data["years"])),
            end_date=str(max(data["years"]) + 1),
            categorisation=categorisation,
        )
        context.log.debug(f"categorization {categorisation} occupancy {occupancy}")
        if occupancy is not None:
            context.emit(person, target=True)
            context.emit(position)
            context.emit(occupancy)


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
        context.log.debug(
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
        context.log.debug(f"Reading PEPs in {lang} for {year} from {info.filename}")
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
