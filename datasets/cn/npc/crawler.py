import re
from urllib.parse import urljoin

from normality import squash_spaces

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element, LangText


REGEX_LABEL = re.compile(r"^(?P<name>[^（(]+?)(?P<annotation>[（(].*)?$")
REGEX_ANNOTATION_SPLIT = re.compile(r"[,，、]")


def parse_annotation(
    context: Context, name: str, annotation: str
) -> tuple[str | None, str | None, str | None]:
    """Parse a parenthesised annotation into (gender, ethnicity, constituency).

    Alongside gender and ethnicity, the annotation can carry a home district
    (ending 区): the Tibet delegation has two deputies both named 拉琼, which the
    source disambiguates this way. Ethnicity is kept in the source language; the
    district is resolved to its English name via the `districts` lookup.
    """

    gender: str | None = None
    ethnicity: str | None = None
    constituency: str | None = None
    # Strip the enclosing parens (a doubled closing paren occurs on 吴海军).
    for part in REGEX_ANNOTATION_SPLIT.split(annotation.strip("（）()")):
        part = part.strip()
        if part == "":
            continue
        if part == "女":
            gender = part
        elif "族" in part:
            ethnicity = part
        elif part.endswith("区"):
            constituency = context.lookup_value("districts", part, warn_unmatched=True)
        else:
            raise ValueError(f"Unknown annotation token for {name}: {part!r}")
    return gender, ethnicity, constituency


def crawl_deputy(
    context: Context,
    deputy: Element,
    delegation: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    label = squash_spaces(h.element_text(deputy)).replace(" ", "")
    match = REGEX_LABEL.match(label)
    assert match is not None
    name = match.group("name")
    annotation = match.group("annotation")

    person = context.make("Person")
    person.id = context.make_id(delegation, name, annotation)

    person.add("name", name, lang="zho")
    apply_translit_full_name(context, person, LangText(name, "chi"))

    # NPC deputies must be PRC citizens: Electoral Law of the National People's
    # Congress and Local People's Congresses, Art. 3 and Art. 15
    # http://en.npc.gov.cn.cdurl.cn/2020-10/17/c_674698.htm
    person.add("citizenship", "cn")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", delegation, lang="zho")

    if annotation is not None:
        gender, ethnicity, constituency = parse_annotation(context, name, annotation)
        person.add("gender", gender)
        person.add("ethnicity", ethnicity, lang="zho")
        occupancy.add("constituency", constituency, lang="eng")

    context.emit(person)
    context.emit(occupancy)


def crawl_delegation(
    context: Context,
    url: str,
    delegation: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    doc = context.fetch_html(url, cache_days=1, encoding="utf-8")
    deputies = h.xpath_elements(doc, ".//div[starts-with(@class, 'md_zi')]")
    for deputy in deputies:
        crawl_deputy(context, deputy, delegation, position, categorisation)
    context.log.info(f"{delegation}: {len(deputies)} deputies")


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        "Member of the National People's Congress",
        country="cn",
        wikidata_id="Q10891456",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Redirect stub pointing at the roster index of the current congress term
    stub = context.fetch_html(context.data_url, cache_days=1, encoding="utf-8")
    refresh = h.xpath_string(stub, ".//meta[@http-equiv='refresh']/@content")
    index_url = urljoin(context.data_url, refresh.split("url=")[1])
    index = context.fetch_html(
        index_url, absolute_links=True, cache_days=1, encoding="utf-8"
    )
    el_links = h.xpath_elements(
        index,
        ".//div[starts-with(@class, 'md_zi')]/a",
        expect_exactly=35,  # 35 electoral delegations
    )
    for el in el_links:
        delegation_name = h.element_text(el)
        delegation_link = el.get("href")
        assert delegation_link is not None
        crawl_delegation(
            context, delegation_link, delegation_name, position, categorisation
        )
