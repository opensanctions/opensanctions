from collections import defaultdict
import shutil
from typing import Dict, Generator, cast
from normality import collapse_spaces
from pantomime.types import HTML
from lxml import html
from lxml.html import HtmlElement
import re

from zavod import Context, helpers as h
from zavod.logic.pep import OccupancyStatus, categorise


REGEX_DELEGATION_HEADING = re.compile(r"(\w+)（\d+名）\[编辑\]$")
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


def crawl_item(
    context: Context,
    input_dict: dict,
):
    name = input_dict.pop("name")
    ethnicity = input_dict.pop("ethnicity")
    gender = input_dict.pop("gender")
    birth_date = input_dict.pop("date_of_birth", None)
    delegation = input_dict.pop("delegation")

    entity = context.make("Person")
    entity.id = context.make_id(name, ethnicity, gender, birth_date, delegation)

    entity.add("name", name, lang="chi")
    entity.add("gender", gender)
    entity.add("ethnicity", ethnicity, lang="chi")
    entity.add("birthDate", h.parse_date(birth_date, formats=["%Y年%m月"]))
    entity.add("political", input_dict.pop("party"))
    entity.add("position", input_dict.pop("position", None), lang="chi")
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

    entity.add("description", input_dict.pop("remarks", None))

    if occupancy:
        context.emit(position)
        context.emit(entity, target=True)
        context.emit(occupancy)

    context.audit_data(input_dict)
    return entity.id


def strip_note(text: str) -> str:
    return REGEX_STRIP_NOTE.sub("", text)


def parse_table(
    context: Context, table: HtmlElement, delegation: str
) -> Generator[Dict[str, str], None, None]:
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
        cells = [
            collapse_spaces(strip_note(el.text_content())) for el in row.findall("./td")
        ]
        row = {hdr: c for hdr, c in zip(headers, cells)}
        row["delegation"] = delegation
        yield row


def crawl(context: Context):
    # curl https://zh.wikipedia.org/wiki/%E7%AC%AC%E5%8D%81%E5%9B%9B%E5%B1%8A%E5%85%A8%E5%9B%BD%E4%BA%BA%E6%B0%91%E4%BB%A3%E8%A1%A8%E5%A4%A7%E4%BC%9A%E4%BB%A3%E8%A1%A8%E5%90%8D%E5%8D%95 -o datasets/cn/wikidata_npc/source.html
    # and check the diff
    h.assert_html_url_hash(
        context,
        context.data_url,
        "e889e12ffbd7cdc60249393a12a899e084514056",
        ".//main",
        text_only=True,
    )
    data_path = context.dataset.base_path / "source.html"
    path = context.get_resource_path("source.html")
    shutil.copyfile(data_path, path)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)
    ids = defaultdict(int)

    for h3 in doc.findall(".//h3"):
        delegation_match = REGEX_DELEGATION_HEADING.match(h3.text_content())
        if not delegation_match:
            continue
        delegation_name = delegation_match.group(1)
        table = h3.getnext().getnext()
        if table.tag != "table":
            table = table.getnext()
        assert table.tag == "table"
        for row in parse_table(context, table, delegation_name):
            id = crawl_item(context, row)
            ids[id] += 1
    context.log.info(f"{len(ids)} unique IDs")
    for id, count in ids.items():
        if count > 1 and id not in IGNORE_DUPES:
            context.log.info(f"ID {id} emitted {count} times")
