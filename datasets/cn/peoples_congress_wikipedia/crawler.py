from collections import defaultdict
from typing import Dict, Generator, cast
from normality import collapse_spaces
from lxml.html import HtmlElement
import re

from zavod import Context, helpers as h
from zavod.logic.pep import OccupancyStatus, categorise


REGEX_DELEGATION_HEADING = re.compile(r"(\w+)（\d+名）$")
REGEX_STRIP_NOTE = re.compile(r"\[註 \d+\]")
SKIP_SUBHEADERS = {
    "中央提名",
    "四川省提名",
    "中央提名",
    "西藏自治区提名",
    "中央提名",
    "陕西省提名",
}
IGNORE_DUPES = {"cn-npc-22933e40a3e1f8cb38f88643263186428150bf6d"}


def clean_text(text: str) -> str:
    return collapse_spaces(strip_note(text))


def crawl_item(
    context: Context,
    input_dict: dict,
    delegation: str,
):
    name = clean_text(input_dict.pop("name").text_content())
    ethnicity = clean_text(input_dict.pop("ethnicity").text_content())
    gender = clean_text(input_dict.pop("gender").text_content())
    birth_date_el = input_dict.pop("date_of_birth", None)
    if birth_date_el is not None:
        birth_date = birth_date_el.text_content()
    else:
        birth_date = None

    entity = context.make("Person")
    entity.id = context.make_id(name, ethnicity, gender, birth_date, delegation)

    entity.add("name", name, lang="chi")
    entity.add("gender", gender)
    entity.add("ethnicity", ethnicity, lang="chi")
    entity.add("birthDate", h.parse_date(birth_date, formats=["%Y年%m月"]))
    party = clean_text(input_dict.pop("party").text_content())
    entity.add("political", party, lang="chi")

    positions = input_dict.pop("position", None)
    if positions is not None:
        for br in positions.xpath(".//br"):
            br.tail = br.tail + "\n" if br.tail else "\n"
        entity.add("position", positions.text_content().split("\n"), lang="chi")
    entity.add(
        "description", delegation + " delegation" if delegation else None, lang="chi"
    )

    position = h.make_position(
        context, "Member of the National People’s Congress", country="cn"
    )
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        False,
        start_date="2023",
        categorisation=categorisation,
        status=OccupancyStatus.UNKNOWN,
    )
    remarks_el = input_dict.pop("remarks", None)
    if remarks_el is not None:
        remarks = clean_text(remarks_el.text_content())
        entity.add("description", remarks, lang="chi")

    if occupancy:
        context.emit(position)
        context.emit(entity, target=True)
        context.emit(occupancy)

    context.audit_data(input_dict)
    return entity.id


def strip_note(text: str) -> str:
    return REGEX_STRIP_NOTE.sub("", text)


def parse_table(
    context: Context, table: HtmlElement
) -> Generator[Dict[str, HtmlElement], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                # Workaround because lxml-stubs doesn't yet support HtmlElement
                # https://github.com/lxml/lxml-stubs/pull/71
                eltree = cast(HtmlElement, el)
                label = strip_note(eltree.text_content())
                headers.append(context.lookup_value("headers", label))
            continue
        # another row of th's after headers has been set
        if row.find("./th") is not None:
            subheader = row.text_content().strip()
            if subheader not in SKIP_SUBHEADERS:
                context.log.warning("Unexpected subheader {subheader}")
            continue
        # populate cells
        cells = row.findall("./td")
        row = {hdr: c for hdr, c in zip(headers, cells)}
        yield row


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    ids = defaultdict(int)

    for h3 in doc.findall(".//h3"):
        delegation_match = REGEX_DELEGATION_HEADING.match(h3.text_content())
        if not delegation_match:
            continue
        delegation_name = delegation_match.group(1)
        table = h3.getparent().getnext().getnext()
        if table.tag != "table":
            table = table.getnext()
        assert table.tag == "table"
        for row in parse_table(context, table):
            id = crawl_item(context, row, delegation_name)
            ids[id] += 1
    context.log.info(f"{len(ids)} unique IDs")
    for id, count in ids.items():
        if count > 1 and id not in IGNORE_DUPES:
            context.log.info(f"ID {id} emitted {count} times")
