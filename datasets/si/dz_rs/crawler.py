import re

from lxml import etree
from rigour.mime.types import XML

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity

# Extract date from bio: middle token is either a numeric month ("9. 12. 1976",
# "12.3.1965", "17. 2.1961", "21.01.1973") or a named month ("8. julija 1955",
# "29. marec 1958"). Numeric alternative is tried first by the alternation.
DOB_RE = re.compile(
    r"Rojen[ao]?\s+(\d{1,2}\.\s*(?:\d{1,2}\.?\s*|[a-zA-Z]+\s+)\d{4})",
    re.IGNORECASE,
)

# Extract birth place after " v ": "Rojen 17. 9. 1958 v Ljubljani." → "Ljubljani"
# Note: place name is in Slovenian locative case (declined form).
BIRTH_PLACE_RE = re.compile(r"\bv\s+(.+?)\.?\s*$")


def parse_birth_date(text: str | None) -> str | None:
    """Extract the date substring from a Slovenian natural-language birth description.

    The ``OSEBA_OSEBNA_IZKAZNICA`` field contains a free-text paragraph with
    the birth date in one of two forms:

    - Named month (genitive/nominative): ``"Rojen 8. julija 1955."``
    - Numeric (various spacing): ``"Rojena 9. 12. 1976"``, ``"Rojena 12.3.1965"``

    The returned string is passed to ``h.apply_date()``, which uses the
    ``dates.months`` and ``dates.formats`` config from the dataset YAML to
    translate Slovenian month names to English and parse all spacing variants.

    Args:
        text: Raw field value; may be ``None`` or contain surrounding prose.

    Returns:
        The extracted date substring (e.g. ``"8. julija 1955"`` or ``"12.3.1965"``)
        ready for ``h.apply_date()``, or ``None`` if no date pattern was found.
    """
    if not text:
        return None
    m = DOB_RE.search(text)
    return m.group(1) if m else None


def parse_birth_place(text: str | None) -> str | None:
    """Extract the birth place from a Slovenian natural-language birth description.

    Splits on `` v `` and returns the remainder, e.g.::

        "Rojen 17. 9. 1958 v Ljubljani." → "Ljubljani"

    Note: the place name is in Slovenian locative case (declined form).

    Args:
        text: Raw ``OSEBA_OSEBNA_IZKAZNICA`` value; may be ``None``.

    Returns:
        The extracted place name, or ``None`` if the pattern is absent.
    """
    if not text:
        return None
    m = BIRTH_PLACE_RE.search(text)
    return m.group(1) if m else None


def crawl_person(
    context: Context,
    oseba: etree._Element,
    position: Entity,
) -> None:
    """Parse one ``<OSEBA>`` element and emit Person + Occupancy entities.

    Args:
        context: Crawler context.
        oseba: The ``<OSEBA>`` XML element for a single deputy.
        position: The shared Position entity for National Assembly members.
    """
    sifra = oseba.findtext("./OSEBA_SIFRA")
    assert sifra is not None, "OSEBA_SIFRA missing"

    person = context.make("Person")
    person.id = context.make_slug("mdb", sifra)

    h.apply_name(
        person,
        first_name=oseba.findtext("./OSEBA_IME"),
        last_name=oseba.findtext("./OSEBA_PRIIMEK"),
    )
    person.add("title", oseba.findtext("./OSEBA_AKADEMSKI_NAZIV"))
    person.add("gender", oseba.findtext("./OSEBA_SPOL"))
    person.add("citizenship", "si")
    person.add("notes", oseba.findtext("./OSEBA_DOSEDANJE_DELO"))

    bio = oseba.findtext("./OSEBA_OSEBNA_IZKAZNICA")
    h.apply_date(person, "birthDate", parse_birth_date(bio))
    person.add("birthPlace", parse_birth_place(bio))

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        propagate_country=True,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)

    position = h.make_position(
        context,
        name="Member of the National Assembly of Slovenia",
        country="si",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21296001",
    )
    context.emit(position)

    for oseba in doc.findall(".//OSEBA"):
        if oseba.findtext("./OSEBA_JE_POSLANEC") != "1":
            continue
        crawl_person(context, oseba, position)
