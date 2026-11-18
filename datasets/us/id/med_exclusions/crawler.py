import re

import pdfplumber
from normality import squash_spaces
from rigour.mime.types import PDF

from zavod import Context
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_resource

# "ANDERSON, LISA LYNN (AKA SMITH)", "MOUSER, CORAL MARLENE (AKA ASHDOWN OR HERNANDEZ)"
REGEX_AKA = re.compile(
    r"^(?P<name>[^()]+?) \(AKA (?P<akas>[A-Z'-]+(?: OR [A-Z'-]+)*)\)$"
)
REGEX_IRREGULAR = re.compile(r"[()‘’]")
# Exclusions cannot predate the 1977 Medicare-Medicaid Anti-Fraud and Abuse Amendments.
TWO_DIGIT_YEAR_BASE = 1977


def clean_cell(value: str | None) -> str | None:
    if value is None:
        return None
    # Cells wrap between words, except for one-off breaks after a hyphen ("10-\nyear").
    text = squash_spaces(value.replace("-\n", "-").replace("\n", " "))
    return text if len(text) > 0 else None


def crawl_item(context: Context, row: dict[str, str | None]) -> None:
    name = row.pop("name")
    start_date = row.pop("start_date")
    assert name is not None and start_date is not None, (name, start_date)

    entity = context.make("LegalEntity")
    # The start date tells apart rows that share a name ("BONE, JOHN"), since
    # nothing shows they are the same person.
    entity.id = context.make_id(name, start_date)
    if (res := context.lookup("names", name)) is not None:
        entity.add_schema("Person")
        h.apply_name(
            entity,
            first_name=res.first_name,
            last_name=res.last_name,
            suffix=res.suffix,
        )
        entity.add("alias", res.alias)
        entity.add("weakAlias", res.weakAlias)
        entity.add("position", res.position)
    elif "," not in name:
        # Organisations stay LegalEntity: the source doesn't say whether they
        # are companies, non-profits or public bodies.
        entity.add("name", name)
        if REGEX_IRREGULAR.search(name):
            context.log.warning("Irregular organisation name", name=name)
    else:
        akas: list[str] = []
        if match := REGEX_AKA.match(name):
            akas = match.group("akas").split(" OR ")
            main_name = match.group("name")
        else:
            main_name = name
        parts = main_name.split(",")
        if len(parts) != 2 or REGEX_IRREGULAR.search(main_name):
            context.log.warning("Irregular person name, add a lookup", name=name)
            entity.add_schema("Person")
            entity.add("name", name)
        else:
            last_name, first_name = parts
            entity.add_schema("Person")
            h.apply_name(entity, first_name=first_name, last_name=last_name)
            # The source gives alternate surnames only, e.g. maiden names.
            for aka in akas:
                h.apply_name(entity, first_name=first_name, last_name=aka, alias=True)
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(
        sanction, "startDate", start_date, two_digit_year_base=TWO_DIGIT_YEAR_BASE
    )
    h.apply_date(
        sanction,
        "endDate",
        row.pop("reinstated"),
        two_digit_year_base=TWO_DIGIT_YEAR_BASE,
    )
    sanction.add("reason", row.pop("information"))
    # The date from which a provider may apply for reinstatement, which does not
    # end the exclusion.
    row.pop("eligible")

    if h.is_active(sanction):
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    _, _, _, path = fetch_resource(
        context,
        "source.pdf",
        context.data_url,
        expected_media_type=PDF,
        geolocation="us",
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    with pdfplumber.open(path) as pdf:
        # The first page explains the list and holds no table.
        assert pdf.pages[0].find_tables() == [], "Table on the cover page"
        for page in pdf.pages[1:]:
            # The PDF draws stray space glyphs over letters ("KARB ER" for "KARBER"),
            # so drop all of them and let pdfplumber infer word gaps from positions.
            page = page.filter(
                lambda obj: (
                    not (obj.get("object_type") == "char" and obj["text"] == " ")
                )
            )
            rows = page.extract_table()
            assert rows is not None, page.page_number
            # The header spans two rows, offset from the data cells, and on later
            # pages begins with the page number: "Page3of8\nName".
            header = [squash_spaces(" ".join(c for c in r if c)) for r in rows[:2]]
            header[0] = re.sub(r"^Page\d+of\d+ ", "", header[0])
            assert header == [
                "Name Start Eligible for Date Additional Information",
                "Date Reinstatement Reinstated",
            ], (page.page_number, header)
            for row in rows[2:]:
                values = [i for i, cell in enumerate(row) if cell is not None]
                assert values == [0, 1, 4, 7, 10], (page.page_number, row)
                crawl_item(
                    context,
                    {
                        "name": clean_cell(row[0]),
                        "start_date": clean_cell(row[1]),
                        "eligible": clean_cell(row[4]),
                        "reinstated": clean_cell(row[7]),
                        "information": clean_cell(row[10]),
                    },
                )
