from collections import defaultdict
from dataclasses import dataclass
from typing import cast
from collections.abc import Generator
from normality import squash_spaces
from lxml.html import HtmlElement
import re

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise
from zavod.shed.trans import apply_translit_full_name
from zavod.util import LangText


REGEX_DELEGATION_HEADING = re.compile(r"(\w+)（\d+名）$")
REGEX_BRACKETS = re.compile(r"\[.*?\]")
CHANGES_IN_REPRESENTATION = [
    "补选",  # by-election
    "辞职",  # resignation
    "罢免",  # dismissal
    "去世",  # death
]
REGEX_STRIP_NOTE = re.compile(r"\[註 \d+\]")
# [Unit] + 提名 = "nominated by [Unit]"
SKIP_SUBHEADERS = {
    "中央提名",
    "四川省提名",
    "中央提名",
    "西藏自治区提名",
    "中央提名",
    "陕西省提名",
    "江苏省提名",
}
IGNORE_DUPES = {
    "cn-npc-22933e40a3e1f8cb38f88643263186428150bf6d",
    # Wang Yonghong is a disambiguation page but both entries on the congress
    # page link to this, and there's no additional information on the congress
    # page to disambiguate by.
    # https://zh.wikipedia.org/wiki/%E7%8E%8B%E6%B0%B8%E7%BA%A2
    "cn-npc-wik-86fc6c8c076a7e4ed86efaec46620d13ea327908",
}


def clean_text(text: str) -> str:
    return squash_spaces(strip_note(text))


def get_cleaned_field(
    input_dict: dict[str, HtmlElement], field_name: str
) -> str | None:
    """Extracts and cleans the field value from the input dictionary."""
    field = input_dict.pop(field_name, None)
    if field is not None:
        text_content = field.text_content()
        # Remove any text in brackets using regex
        cleaned_text = re.sub(REGEX_BRACKETS, "", text_content)
        return clean_text(cleaned_text)

    return None


def crawl_item(
    context: Context,
    input_dict: dict[str, HtmlElement],
    delegation: str | None,
) -> str | None:
    name_el = input_dict.pop("name")
    reference = name_el.find(".//sup")
    # Make sure to explicitly check if the element is not None.
    # Avoid using implicit truthiness, as it may lead to incorrect results in future versions.
    if reference is not None:
        reference.getparent().remove(reference)
    name = name_el.text_content()
    ethnicity = clean_text(input_dict.pop("ethnicity").text_content())
    gender = clean_text(input_dict.pop("gender").text_content())
    # Keep the original delegation for the current members, pop it for the rest
    if delegation is None:
        delegation = clean_text(input_dict.pop("delegation").text_content())
    date_of_by_election = get_cleaned_field(input_dict, "date_of_by_election")
    date_of_death = get_cleaned_field(input_dict, "date_of_death")
    date_of_resignation = get_cleaned_field(input_dict, "date_of_resignation")
    date_of_dismissal = get_cleaned_field(input_dict, "date_of_dismissal")

    birth_date_el = input_dict.pop("date_of_birth", None)
    if birth_date_el is not None:
        birth_date = birth_date_el.text_content().strip()
    else:
        birth_date = None

    entity = context.make("Person")
    entity.id = context.make_id(name, ethnicity, gender, birth_date, delegation)
    # PRC citizenship required even for HK and Macau NPC Delegates: http://en.npc.gov.cn.cdurl.cn/2020-10/17/c_674698.htm
    entity.add("citizenship", "cn")

    entity.add("name", name, lang="chi")
    apply_translit_full_name(context, entity, LangText(name, "chi"))
    entity.add("gender", gender)
    entity.add("ethnicity", ethnicity, lang="chi")
    h.apply_date(entity, "birthDate", birth_date)
    h.apply_date(entity, "deathDate", date_of_death)
    party = clean_text(input_dict.pop("party").text_content())
    entity.add("political", party, lang="chi")

    positions = input_dict.pop("position", None)
    if positions is not None:
        for br in positions.xpath(".//br"):
            br.tail = br.tail + "\n" if br.tail else "\n"
        entity.add("position", positions.text_content().split("\n"), lang="chi")

    position = h.make_position(
        context, "Member of the National People’s Congress", country="cn"
    )
    categorisation = categorise(context, position, default_is_pep=True)

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        False,
        start_date=date_of_by_election or "2023",
        end_date=date_of_resignation or date_of_dismissal or date_of_death,
        categorisation=categorisation,
    )
    remarks_el = input_dict.pop("remarks", None)
    if remarks_el is not None:
        remarks = clean_text(remarks_el.text_content())
        entity.add("description", remarks, lang="chi")

    if occupancy:
        # The delegation (代表团) is the unit the delegate is elected to
        # represent in the NPC. It's usually a province/region, but sometimes
        # the military (解放军和武警部队), so stuffing it into constituency -
        # defined as a geographic area - is somewhat of a misuse of the field.
        occupancy.add("constituency", delegation, lang="chi")
        context.emit(position)
        context.emit(entity)
        context.emit(occupancy)

    context.audit_data(
        input_dict,
        # Since we're adding same position for all, we don't need these fields
        ignore=[
            "position_before_death",
            "position_before_resignation",
            "position_before_dismissal",
        ],
    )
    return entity.id


def strip_note(text: str) -> str:
    return REGEX_STRIP_NOTE.sub("", text)


@dataclass
class PendingRowspan:
    """A cell that spans into subsequent rows via its rowspan attribute."""

    element: HtmlElement
    remaining: int


def expand_rowspan_cells(
    row: HtmlElement, rowspans: dict[int, PendingRowspan], num_columns: int
) -> list[HtmlElement]:
    """Reconstruct a row's cells, filling in cells carried over by a rowspan
    from a row above.

    The source HTML omits the <td>s covered by a rowspan from the rows below it,
    so a naive zip with the headers would shift every following value one column
    to the left. ``rowspans`` tracks the carry-over state across rows (keyed by
    column index) and is mutated in place.
    """
    cells: list[HtmlElement] = []
    td_iter = iter(row.findall("./td"))
    for col in range(num_columns):
        # A rowspan from a row above occupies this column: reuse its cell.
        pending = rowspans.get(col)
        if pending is not None:
            cells.append(pending.element)
            pending.remaining -= 1
            if pending.remaining == 0:
                del rowspans[col]
            continue
        # Otherwise consume the next actual <td> in this row.
        cell = next(td_iter, None)
        if cell is None:
            break
        cells.append(cell)
        colspan = int(cell.get("colspan", "1"))
        assert colspan == 1, ("colspan in data row not supported", row)
        rowspan = int(cell.get("rowspan", "1"))
        if rowspan > 1:
            rowspans[col] = PendingRowspan(cell, rowspan - 1)
    return cells


def parse_table(
    context: Context, table: HtmlElement
) -> Generator[dict[str, HtmlElement], None, None]:
    headers = None
    rowspans: dict[int, PendingRowspan] = {}
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                # Workaround because lxml-stubs doesn't yet support HtmlElement
                # https://github.com/lxml/lxml-stubs/pull/71
                eltree = cast(HtmlElement, el)
                label = strip_note(eltree.text_content())
                header = context.lookup_value("headers", label)
                # An unmapped header would silently shift every value into the
                # wrong column, so fail loudly and force a lookup to be added.
                assert header is not None, ("Unmapped table header", label)
                headers.append(header)
            continue
        # another row of th's after headers has been set
        if row.find("./th") is not None:
            subheader = row.text_content().strip()
            if subheader not in SKIP_SUBHEADERS:
                context.log.warning(f"Unexpected subheader {subheader}")
            continue
        cells = expand_rowspan_cells(row, rowspans, len(headers))
        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    ids: defaultdict[str | None, int] = defaultdict(int)

    for h3_el in doc.findall(".//h3"):
        # Workaround because lxml-stubs doesn't yet support HtmlElement
        # https://github.com/lxml/lxml-stubs/pull/71
        h3 = cast(HtmlElement, h3_el)
        h3_text = h3.text_content().strip()
        delegation_match = REGEX_DELEGATION_HEADING.match(h3_text)
        heading_parent = h3.getparent()
        assert heading_parent is not None, h3_text
        # Determine whether to process the <h3> based on delegation match or specific headings
        if delegation_match:
            delegation_name = delegation_match.group(1)
            sibling = heading_parent.getnext()
            assert sibling is not None, h3_text
            table = sibling.getnext()
        elif any(heading in h3_text for heading in CHANGES_IN_REPRESENTATION):
            delegation_name = None
            table = heading_parent.getnext()
        else:
            continue
        # There are cases where the table is not immediately after the <h3>
        # so we look until we get the next <table> element
        while table is not None and table.tag != "table":
            assert not table.tag.startswith("h"), table
            table = table.getnext()
        assert table is not None, h3_text
        assert table.tag == "table"
        for row in parse_table(context, table):
            id = crawl_item(context, row, delegation_name)
            ids[id] += 1

    context.log.info(f"{len(ids)} unique IDs")
    for id, count in ids.items():
        if count > 1 and id not in IGNORE_DUPES:
            context.log.info(f"ID {id} emitted {count} times")
