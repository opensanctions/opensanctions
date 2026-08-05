import re
from collections import defaultdict

import pdfplumber
from pdfplumber.page import Page
from rigour.mime.types import PDF

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")

# The deputy list is a PDF laid out as a ruled table. Rather than hard-coding pixel
# offsets, we derive the column boundaries from the table's own vertical ruling lines,
# which sit at stable positions on every page. The far-left "région" label is set in
# rotated text that produces spurious vertical edges, so we only consider ruling lines
# at x >= COLUMN_XMIN, i.e. from the CIRCONSCRIPTION column rightwards.
COLUMN_XMIN = 155.0
EDGE_CLUSTER = 8.0  # points; vertical ruling lines closer than this are one boundary
# Columns left-to-right once the "région" label is dropped.
COLUMNS = ("constituency", "party", "last_name", "first_name", "dob", "birth_place")


def column_separators(page: Page) -> list[float]:
    """Cluster the page's vertical ruling lines into column boundary x-positions."""
    xs = sorted(
        float(edge["x0"])
        for edge in page.edges
        if edge["orientation"] == "v" and float(edge["x0"]) >= COLUMN_XMIN
    )
    separators: list[float] = []
    cluster: list[float] = []
    for x in xs:
        if cluster and x - cluster[-1] > EDGE_CLUSTER:
            separators.append(sum(cluster) / len(cluster))
            cluster = []
        cluster.append(x)
    if cluster:
        separators.append(sum(cluster) / len(cluster))
    return separators


def extract_rows(page: Page) -> list[list[str]]:
    """Extract the deputy table as rows of trimmed, single-line cells.

    Each cell's wrapped lines are collapsed to a single whitespace-separated string.
    """
    settings = {
        "vertical_strategy": "explicit",
        "explicit_vertical_lines": column_separators(page),
        "horizontal_strategy": "lines",
    }
    tables = page.extract_tables(settings)
    if len(tables) == 0:
        return []
    table = max(tables, key=len)
    return [[re.sub(r"\s+", " ", cell or "").strip() for cell in row] for row in table]


def validate_header(rows: list[list[str]], header_labels: dict[str, str]) -> None:
    """Assert every column carries its expected header label, else crash.

    The header spans several rows and its labels sit above the first data row (the
    first row bearing a date of birth), so accumulate the text per column until then.
    """
    header: dict[int, list[str]] = defaultdict(list)
    for row in rows:
        if any(DATE_RE.match(cell) for cell in row):
            break
        for index, cell in enumerate(row):
            if cell:
                header[index].append(cell.upper())
    for index, key in enumerate(COLUMNS):
        label = header_labels[key]
        joined = " ".join(header.get(index, []))
        if label not in joined:
            raise ValueError(
                f"Unexpected column {index}: want {label!r}, found {joined!r}"
            )


def crawl_deputy(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, str],
    constituency: str | None,
) -> None:
    name = f"{row['last_name']} {row['first_name']}".strip()
    if not name:
        raise ValueError(f"Empty deputy name near DOB {row['dob']!r}")
    party = row["party"]

    person = context.make("Person")
    person.id = context.make_id(name, row["dob"])
    h.apply_name(
        person,
        first_name=row["first_name"] or None,
        last_name=row["last_name"] or None,
        lang="fra",
    )
    h.apply_date(person, "birthDate", row["dob"])
    person.add("birthPlace", row["birth_place"] or None, lang="fra")
    person.add("political", party or None, lang="fra")
    # A candidate for the National Assembly must be Ivorian by birth and never have
    # renounced Ivorian nationality (Electoral Code, Loi 2000-514, Article 71).
    # https://aceproject.org/ero-en/regions/africa/CI/cote-divoire-electoral-law-nb0-2000-514-of-1/at_download/file
    person.add("citizenship", "ci")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency, lang="fra")
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Côte d'Ivoire",
        country="ci",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295981",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    path = context.fetch_resource("deputies.pdf", context.data_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    header_labels: dict[str, str] = context.dataset.config["header_labels"]
    with pdfplumber.open(path) as pdf:
        # The header only appears on the first page; validate it there, then apply the
        # same column model to every page.
        validate_header(extract_rows(pdf.pages[0]), header_labels)
        constituency: str | None = None
        for page in pdf.pages:
            for cells in extract_rows(page):
                if len(cells) != len(COLUMNS):
                    raise ValueError(f"Unexpected column count in row: {cells!r}")
                row: dict[str, str] = dict(zip(COLUMNS, cells))
                if not DATE_RE.match(row["dob"]):
                    continue  # header or spacer row, not a titular deputy
                # Multi-seat constituencies list their localities once, on the first
                # deputy's row; carry that forward to the co-elected deputies below.
                if row["constituency"]:
                    constituency = row["constituency"]
                crawl_deputy(context, position, categorisation, row, constituency)
