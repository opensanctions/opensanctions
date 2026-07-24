import re

from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.stateful.review import assert_all_accepted

from zavod import Context
from zavod import helpers as h

# Names appear only in portrait file names, e.g. HON.-RATU-ATONIO-LALABALAVU-1-300x300.jpg.
# Used only to tell member portraits from other images on the page.
FILENAME_RE = re.compile(r"(?i)^hon\.?-.+")


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

    # Names are too messily encoded to clean in code, so we minimally decode and hand
    # every name to the review framework for cleaning.
    stem = filename_raw.rsplit(".", 1)[0]
    name = " ".join(stem.replace("-", " ").split())
    h.apply_reviewed_names(
        context,
        person,
        original=h.Names(name=name),
        is_irregular=True,
        llm_cleaning=True,
    )

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

    assert_all_accepted(context, raise_on_unaccepted=False)
