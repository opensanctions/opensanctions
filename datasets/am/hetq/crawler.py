"""
Crawler for PEP data downloaded from data.hetq.am
"""

import itertools
import json
import re
from typing import Dict, List, Literal, Any, Optional, Tuple, Union
from zipfile import ZipFile

from lxml.html import document_fromstring, HtmlElement

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.shed.internal_data import fetch_internal_data

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
HY_BIRTHDATE = re.compile(
    r"ծնվել է (\d+)\s*(?:թվականին|թվականի|թ.)(?:,?\s+(\S+)\s+(\d+)\s*[–-]\s*ին)?", re.I
)
EN_BIRTH = re.compile(
    r"born\s+(?:on\s+)?(\S+)\s+(\d+)\s*(?:,|in)\s*(\d+)(?:\s*in\s+([^\.]+))?", re.I
)


def fix_trans_spill(text: str) -> str:
    """
    Drop remaining text when encountering apparent spillage of some automatic
    translation tool response that looks a bit like

        ... Translations of prosecutor NounFrequency մեղադրող ...
    """
    return text.split("Translations of")[0].strip()


def get_birth_info(
    context: Context, zipfh: ZipFile, person_id: int
) -> Tuple[Union[None, str], Union[None, str]]:
    """Get birth date and place from person page."""
    # We might have valid HTML, but we might also have an incomplete
    # page with a "Parse Error".  In any case we are really just
    # looking for date and place of birth, so use the text.
    doc: HtmlElement = document_fromstring(
        zipfh.read(f"hetq-data/person/{person_id}-en.html")
    )
    english = doc.text_content()
    # Try to get date / place in English if possible
    birth_date = birth_place = None
    m = EN_BIRTH.search(english)
    if m:
        context.log.debug(f"Found English birth info for {person_id}: {m.groups()}")
        month, day, year, birth_place = m.groups()
        birth_date = " ".join((month, day, year))
    else:
        doc = document_fromstring(zipfh.read(f"hetq-data/person/{person_id}-hy.html"))
        armenian = doc.text_content()
        # In Armenian we don't use the place of birth because it is in
        # locative case and we don't have a lemmatizer.
        m = HY_BIRTHDATE.search(armenian)
        if m:
            context.log.debug(f"Found Armenian birthdate for {person_id}: {m.groups()}")
            year, month, day = m.groups()
            birth_date = year
            if month is not None:
                for idx, monthname in enumerate(MONTHS):
                    if month.startswith(monthname):
                        birth_date = f"{year}-{idx + 1}-{day}"
                        break
    return birth_date, birth_place


def crawl_person(
    context: Context,
    zipfh: ZipFile,
    person_id: int,
    data: Dict[str, Any],
) -> Optional[Entity]:
    """Create person and position/occupancy if applicable."""
    birth_date, birth_place = get_birth_info(context, zipfh, person_id)
    name_en = data.get("name_en", "").strip()
    name_hy = data.get("name_hy", "").strip()
    if h.is_empty(name_en) and h.is_empty(name_hy):
        context.log.warning(f"Skipping person {person_id} with no name")
        return None
    person = context.make("Person")
    person.id = context.make_slug(str(person_id))
    context.log.debug(f"Unique ID {person.id} for {person_id} ({name_en} / {name_hy})")
    if name_en:
        person.add("name", name_en, lang="en")
    if name_hy:
        person.add("name", name_hy, lang="hy")
    names = person.get("name")
    if any("null null null" in name for name in names):
        context.log.warning(
            f"Person {person_id} has 'null null null' in name", names=names
        )
    if birth_date is not None:
        h.apply_date(person, "birthDate", birth_date)
    if birth_place is not None:
        person.add("birthPlace", birth_place)
    position_name = data.get("position_en", "").strip()
    position_name = fix_trans_spill(position_name)
    position_lang = "en"
    # Often position is only in Armenian
    if position_name == "":
        position_name = data.get("position_hy", "").strip()
        position_lang = "hy"
    if position_name:
        context.log.debug(f"position: {position_name} years: {data['years']}")
        position = h.make_position(
            context,
            name=position_name,
            country="am",
            lang=position_lang,
        )
        # Do usual PEP logic for positions/occupancies
        categorisation = categorise(context, position, is_pep=True)
        if categorisation.is_pep:
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                start_date=str(min(data["years"])),
                # last year of activity is not necessarily when they departed the
                # position, it's just as likely the last year in which the site was
                # updated.
                end_date=None,
                no_end_implies_current=False,
                categorisation=categorisation,
            )
            context.log.debug(f"categorization {categorisation} occupancy {occupancy}")
            if occupancy is not None:
                context.emit(occupancy)
                context.emit(position)
    # Emit all the persons anyway
    person.add("topics", "poi")
    context.emit(person)
    return person


def crawl_list(
    context: Context,
    peps: Dict[int, Dict[str, Any]],
    data: List[Dict[str, Any]],
    year: int,
    lang: SupportedLanguage,
):
    """Accumulate person info from yearly list.  Do not actually
    create entities yet because they are duplicated in multiple years."""
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


def crawl_missing_pois(
    context: Context,
    zipfh: ZipFile,
    missing_pois: Dict[int, Any],
    persons: Dict[int, Dict[str, Any]],
):
    """Create an entity for a person of interest not named in the
    front-page list of PEPs."""
    # We should have downloaded these in download.py (at first we didn't)
    for person_id, data in missing_pois.items():
        if person_id in persons:
            context.log.debug(f"Already crawled missing POI {person_id}")
            continue
        persons[person_id] = data
        person = crawl_person(context, zipfh, person_id, data)
        if person is not None:
            data["entity"] = person


def crawl_relations(
    context: Context, zipfh: ZipFile, persons: Dict[int, Dict[str, Any]], person_id: int
):
    """Read relation graph and create entities."""
    # There should always be a person / entity for the source
    assert person_id in persons
    assert "entity" in persons[person_id]
    graph_en = json.loads(zipfh.read(f"hetq-data/relations/{person_id}-en.json"))
    if not graph_en:
        context.log.debug(
            f"Graph from hetq-data/relations/{person_id}-en.json is empty"
        )
        graph_en = {"nodes": [], "edges": []}
    graph_hy = json.loads(zipfh.read(f"hetq-data/relations/{person_id}-hy.json"))
    if not graph_hy:
        context.log.debug(
            f"Graph from hetq-data/relations/{person_id}-hy.json is empty"
        )
        graph_hy = {"nodes": [], "edges": []}
    # Node IDs *should* be "n{person_id}" but let's map them to be
    # sure.  Also many of these persons will not be front-page PEPs so
    # find those.  Also do both languages to be sure.
    node_to_person = {}
    missing_pois = {}
    for n in graph_en["nodes"]:
        node_person_id = n["personID"]
        node_to_person[n["id"]] = node_person_id
        if node_person_id not in persons:
            missing_pois[node_person_id] = {"name_en": n["label"]}
    for n in graph_hy["nodes"]:
        node_person_id = n["personID"]
        assert node_to_person[n["id"]] == node_person_id
        if node_person_id not in persons:
            if node_person_id not in missing_pois:
                context.log.debug("Person {node_person_id} only shown in Armenian")
                missing_pois[node_person_id] = {}
            missing_pois[node_person_id]["name_hy"] = n["label"]
    # Only person IDs for PEPs are actually named in the front-page
    # list, but we will include their families as POIs as well.
    crawl_missing_pois(context, zipfh, missing_pois, persons)
    seen = set()
    for e in itertools.chain(graph_en["edges"], graph_hy["edges"]):
        source = node_to_person.get(e.get("source"))
        if source is None:
            context.log.warning(f"Unknown source node in edge: {e}")
            continue
        target = node_to_person.get(e.get("target"))
        if target is None:
            context.log.warning(f"Unknown target node in edge: {e}")
            continue
        if (source, target) in seen:
            context.log.debug(f"Relation {source}:{target} already exists")
            continue
        seen.add((source, target))
        relationship = e.get("label")
        if relationship is None:
            context.log.info(f"Skipping relation {person_id}:{target} has no label")
            continue
        context.log.debug(f"Relation {source}:{target}:{relationship}")
        # There are only family relations it seems
        relation = context.make("Family")
        relation.id = context.make_slug("relation", str(source), str(target))
        relation.add("person", persons[source]["entity"])
        relation.add("relative", persons[target]["entity"])
        relation.add("relationship", relationship)
        context.emit(relation)


def crawl_lists(context: Context, zipfh: ZipFile):
    """Read lists of persons for each year covered, matching names and
    accumulating years in which the person was active."""
    persons: Dict[int, Dict[str, Any]] = {}
    for info in zipfh.infolist():
        # zipfile.Path would be good for this but it may not work
        # correctly in all versions of Python
        m = re.match(r".*front/(\d\d\d\d)-(hy|en)\.json$", info.filename)
        if m is None:
            continue
        year_str, lang = m.groups()
        year = int(year_str)
        context.log.debug(f"Reading persons in {lang} for {year} from {info.filename}")
        with zipfh.open(info.filename) as infh:
            data = json.load(infh)
            crawl_list(context, persons, data, year, lang)  # type: ignore
    # Now that we have the persons we can grab some personal data and
    # create Persons
    for person_id, data in persons.items():
        person = crawl_person(context, zipfh, person_id, data)
        if person is not None:
            persons[person_id]["entity"] = person
    # And create relations between entities
    for person_id in list(persons.keys()):
        crawl_relations(context, zipfh, persons, person_id)


def crawl(context: Context):
    """Download the zip of Hetq data and create Person entities."""

    data_path = context.get_resource_path("hetq-data.zip")
    fetch_internal_data("am_hetq_peps/20240422/hetq-data.zip", data_path)
    with ZipFile(data_path) as zipfh:
        crawl_lists(context, zipfh)
