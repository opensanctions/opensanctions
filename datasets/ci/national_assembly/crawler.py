import re

import pdfplumber
from rigour.mime.types import PDF

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

# The Apache server returns 403 without a browser User-Agent and a same-site Referer.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.assnat.ci/",
}

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")

# The list is a fixed-column PDF. Each titular deputy row is anchored on a date-of-birth
# cell; the surrounding columns sit at stable horizontal positions (measured in points
# from the left edge). The vertical "région" label on the far left (x < 160) is ignored.
PARTY_X = (360, 440)  # GRP/PARTI abbreviation
NAME_X = (440, 656)  # NOM + PRÉNOMS, between the party and the date of birth
BIRTHPLACE_X = 690  # LIEU DE NAISSANCE, to the right of the date of birth
ROW_TOLERANCE = 6  # points; words within this vertical distance are one row

# Known political groups / parties, used to flag unexpected values loudly.
KNOWN_PARTIES = {"RHDP", "PDCI-RDA", "INDEPENDANT", "UNPR", "FPI", "LE BUFFLE"}


def cell(words: list[dict[str, float]], lo: float, hi: float) -> str:
    return " ".join(str(w["text"]) for w in words if lo <= float(w["x0"]) < hi).strip()


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Côte d'Ivoire",
        country="ci",
        wikidata_id="Q21295981",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    path = context.fetch_resource("deputies.pdf", context.data_url, headers=HEADERS)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    count = 0
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            words = page.extract_words()
            dob_words = [w for w in words if DATE_RE.match(str(w["text"]))]
            for dob in dob_words:
                top = float(dob["top"])
                band = sorted(
                    (w for w in words if abs(float(w["top"]) - top) < ROW_TOLERANCE),
                    key=lambda w: float(w["x0"]),
                )
                name = cell(band, *NAME_X)
                party = cell(band, *PARTY_X)
                birth_place = cell(band, BIRTHPLACE_X, 100000)
                assert name, f"Empty deputy name near DOB {dob['text']!r}"
                if party and party not in KNOWN_PARTIES:
                    context.log.warning("Unknown party/group", party=party, name=name)

                person = context.make("Person")
                person.id = context.make_id(name, str(dob["text"]))
                person.add("name", name, lang="fra")
                h.apply_date(person, "birthDate", str(dob["text"]))
                person.add("birthPlace", birth_place or None, lang="fra")
                person.add("political", party or None, lang="fra")
                # A candidate for the National Assembly must be Ivorian by birth and never
                # have renounced Ivorian nationality (Electoral Code, Loi 2000-514,
                # Article 71). https://aceproject.org/ero-en/regions/africa/CI/cote-divoire-electoral-law-nb0-2000-514-of-1/at_download/file
                person.add("citizenship", "ci")

                occupancy = h.make_occupancy(
                    context, person, position, categorisation=categorisation
                )
                if occupancy is None:
                    continue
                context.emit(occupancy)
                context.emit(person)
                count += 1

    if count == 0:
        raise ValueError("No deputies parsed from the National Assembly PDF")
