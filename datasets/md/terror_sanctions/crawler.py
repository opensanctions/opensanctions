import re
from lxml import html
from rigour.mime.types import HTML

from zavod import Context
from zavod import helpers as h

# UN Security Council permanent reference numbers, e.g. QDi.436, TAe.010, KPe.075.
# The two-letter prefix identifies the sanctions committee, the "i"/"e" suffix
# distinguishes individuals from entities.
UNSC_ID_REGEX = re.compile(r"^[A-Z]{2}[ie]\.\d{3,}$")

SPLITS = [
    "si",
    ";",
    "sau",
    "a)",
    "b)",
    "c)",
    "d)",
    "Aproximativ ",
    "Intre",
    "între",
    "și",
    "la",
    "din pasaport fals",
    "presupusă:",
]


def clean_name(string: str) -> str:
    name = re.sub(r"^[\]\), ]+", "", string)
    name = re.sub(r"[\[\(\., ]+$", "", string)
    return name


def parse_names(string: str) -> tuple[str, list[str]]:
    parts = string.split("alias")
    name = clean_name(parts[0])
    aliases = parts[1:] if len(parts) > 1 else []
    aliases = [clean_name(alias) for alias in aliases]
    return name, aliases


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.html", context.data_url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path) as fh:
        doc = html.parse(fh)

    table = doc.find(".//table")
    for row in h.parse_html_table(table):
        str_row = h.cells_to_str(row)
        dob = str_row.pop("data_de_nastere")
        entity = context.make("LegalEntity")
        persoana = str_row.pop("persoana_fizica_entitate")
        assert persoana is not None
        name, aliases = parse_names(persoana)
        entity.id = context.make_id(name, dob)
        entity.add("name", name)
        entity.add("topics", "sanction")
        if aliases:
            entity.add("alias", aliases)
        for date in h.multi_split(dob, SPLITS):
            entity.add_schema("Person")
            h.apply_date(entity, "birthDate", date)

        sanction = h.make_sanction(context, entity)
        ref_num = str_row.pop("numar_de_referinta") or None
        if ref_num is not None:
            if UNSC_ID_REGEX.match(ref_num):
                sanction.add("unscId", ref_num)
            else:
                # Row numbers within the annexes, or resolution references for
                # the lists which don't use UNSC permanent reference numbers.
                sanction.add("authorityId", ref_num)
        sanction.add("program", str_row.pop("sanctiuni_teroriste") or None, lang="mol")
        sanction.add(
            "program", str_row.pop("sanctiuni_de_proliferare") or None, lang="mol"
        )

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(str_row)
