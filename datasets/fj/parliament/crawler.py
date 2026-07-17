import re

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

# The parliament site is served behind a Sucuri anti-bot proxy that requires solving a
# JavaScript challenge, so the page is fetched through the Zyte API (browser rendering).
# Member photos being present signals the challenge was cleared successfully.
PHOTO_XPATH = './/img[contains(@src, "300x300")]'

# The site does not expose member names as text; each member is shown only as a portrait
# whose file name encodes the name, e.g.
#   HON.-RATU-ATONIO-LALABALAVU-1-300x300.jpg  ->  Ratu Atonio Lalabalavu
# The convention is consistent: an "HON." prefix, dash-separated name words, an optional
# trailing sequence number and the "-300x300" thumbnail dimension.
FILENAME_RE = re.compile(r"(?i)^hon\.?-.+")


def clean_name(filename: str) -> str:
    name = filename.rsplit(".", 1)[0]  # drop file extension
    name = re.sub(r"-?\d+x\d+$", "", name)  # drop the "-300x300" dimension
    name = re.sub(r"(?i)^hon\.?-", "", name)  # drop the "HON.-" prefix
    name = re.sub(r"\d+$", "", name)  # drop a trailing sequence number
    name = re.sub(r"-+", " ", name).strip()  # dashes to spaces
    return name.title()


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
        unblock_validator=PHOTO_XPATH,
        cache_days=1,
    )

    seen: set[str] = set()
    for src in h.xpath_strings(doc, "//img/@src"):
        filename = src.rsplit("/", 1)[-1]
        if FILENAME_RE.match(filename) is None:
            continue
        name = clean_name(filename)
        # The file-name convention always yields at least a given name and a surname; a
        # single token signals an unexpected file name and should fail loudly.
        if len(name.split()) < 2:
            raise ValueError(f"Could not parse member name from image: {filename!r}")
        if name in seen:
            continue
        seen.add(name)

        person = context.make("Person")
        person.id = context.make_id(name)
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
            continue
        context.emit(occupancy)
        context.emit(person)

    if not seen:
        raise ValueError("No member portraits found on the Fiji members page")
