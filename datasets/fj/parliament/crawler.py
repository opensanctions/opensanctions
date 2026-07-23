import re

from rigour.names import remove_person_prefixes
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context
from zavod import helpers as h

# The site does not expose member names as text; each member is shown only as a portrait
# whose file name encodes the name, e.g.
#   HON.-RATU-ATONIO-LALABALAVU-1-300x300.jpg  ->  Ratu Atonio Lalabalavu
# The convention is consistent: an "HON." prefix, dash-separated name words, an optional
# trailing sequence number and the "-300x300" thumbnail dimension.
FILENAME_RE = re.compile(r"(?i)^hon\.?-.+")


def clean_name(filename: str) -> str:
    name = filename.rsplit(".", 1)[0]  # drop file extension
    name = re.sub(r"-?\d+x\d+$", "", name)  # drop the "-300x300" dimension
    name = re.sub(r"\d+$", "", name)  # drop a trailing sequence number
    name = re.sub(r"-+", " ", name).strip()  # dashes to spaces
    # `remove_person_prefixes` drops honorific prefixes ("Hon.", "Dr", ...) using a
    # maintained list, while preserving the Fijian chiefly titles Ratu, Adi and Ro,
    # which are part of the name.
    return remove_person_prefixes(name.title())


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    src: str,
) -> None:
    filename_raw = src.rsplit("/", 1)[-1]
    if FILENAME_RE.match(filename_raw) is None:
        return
    name = clean_name(filename_raw)
    # The file-name convention always yields at least a given name and a surname; a
    # single token signals an unexpected file name and should fail loudly.
    if len(name.split()) < 2:
        context.log.warning(f"Unexpected member file name: {filename_raw}")
        return

    person = context.make("Person")
    # use raw filename for id, not the cleaned name
    person.id = context.make_id(filename_raw)
    person.add("name", name)
    # A candidate for Parliament must be a citizen of Fiji and hold no other
    # citizenship (2013 Constitution of Fiji, Section 56(2)(a)).
    # https://www.constituteproject.org/constitution/Fiji_2013
    person.add("citizenship", "fj")

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
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator='.//img[contains(@src, "300x300")]',
        cache_days=1,
    )

    for source_attr in h.xpath_strings(doc, "//img/@src"):
        crawl_member(context, position, categorisation, source_attr)
