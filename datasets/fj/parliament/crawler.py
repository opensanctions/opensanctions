import re

from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context
from zavod import helpers as h

# Names appear only in portrait file names, e.g. HON.-RATU-ATONIO-LALABALAVU-1-300x300.jpg.
# Used to tell member portraits from other images on the page.
FILENAME_RE = re.compile(r"(?i)^hon\.?-.+")

# File names carry the portrait's image dimensions (e.g. "300x300") and often a
# WordPress dedup counter ("1", "03", "003"), sometimes glued to the last name
# word ("Cirikiyasawa2"). Strip these trailing digit artifacts before review.
ARTIFACT_RE = re.compile(r"(?:\s*\d+\s*x\s*\d+|\s*\d+)\s*$")


def strip_filename_artifacts(name: str) -> str:
    """Remove trailing image dimensions and dedup counters from a filename-derived name."""
    prev = None
    while prev != name:
        prev = name
        name = ARTIFACT_RE.sub("", name).strip()
    return name


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    src: str,
) -> None:
    filename_raw = src.rsplit("/", 1)[-1]
    if FILENAME_RE.match(filename_raw) is None:
        return

    person = context.make("Person")
    # Key on the raw file name: the only stable identifier the source gives us.
    person.id = context.make_id(filename_raw)
    # MPs must be Fiji citizens holding no other citizenship (2013 Constitution, s. 56(2)(a)).
    person.add("citizenship", "fj")

    # Decode the file name into a name, dropping the "HON" title and the trailing
    # image-dimension/dedup-counter digit artifacts, then hand the residual (which
    # may still need case/ordering fixes) to the review framework for cleaning.
    stem = filename_raw.rsplit(".", 1)[0]
    raw_name = " ".join(stem.replace("-", " ").split())
    untitled_name = h.strip_name_titles(context, raw_name)
    if untitled_name is None:
        return
    clean_name = strip_filename_artifacts(untitled_name)
    person.add("name", clean_name)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Fiji",
        country="fj",
        wikidata_id="Q18145348",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator='.//img[contains(@src, "300x300")]',
        cache_days=14,
    )

    for source_attr in h.xpath_strings(doc, "//img/@src"):
        crawl_member(context, position, categorisation, source_attr)
