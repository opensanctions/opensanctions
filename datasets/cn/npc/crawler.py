import re
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.shed.trans import ENGLISH, apply_translit_full_name
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

# The entry URL is a stub page whose meta-refresh points at the roster index
# of the current congress term (e.g. /npc/c191/dbmd/dbmd14/).
REGEX_REDIRECT = re.compile(r"url=(?P<path>/npc/c191/dbmd/dbmd(?P<term>\d+)/)")
# Gender/ethnicity annotation, e.g. (女), (满族), (女,回族). Both half- and
# full-width parentheses and comma variants occur across delegation pages.
REGEX_ANNOTATION = re.compile(r"^[（(](?P<parts>[^()（）]*)[)）]$")
REGEX_ANNOTATION_SPLIT = re.compile(r"[,，、]")
# Short names are padded for alignment with a mix of regular spaces, EN SPACE
# (U+2002) and ideographic space (U+3000); all of these match \s.
REGEX_WHITESPACE = re.compile(r"\s+")

# When the next congress convenes (15th NPC, expected March 2028), the stub
# starts pointing at its roster and the crawler fails loudly below, forcing a
# review of the page structure before the new term is crawled.
EXPECTED_TERM = 14


def crawl_deputy(
    context: Context,
    div: Element,
    delegation: str,
    term: int,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    cls = div.get("class")
    # md_zi2 is a wide-cell variant used for long (mostly non-Han) names.
    if cls not in ("md_zi", "md_zi2"):
        raise ValueError(f"Unexpected deputy element class: {cls!r}")
    if div.find(".//a") is not None:
        raise ValueError(f"Deputy entry contains a link: {h.element_text(div)!r}")

    annotation = ""
    span = div.find("./span")
    if span is not None:
        annotation = h.element_text(span)
        div.remove(span)
    name = REGEX_WHITESPACE.sub("", h.element_text(div, squash=False))
    if name == "":
        raise ValueError(f"Empty deputy name in {delegation} delegation")

    gender: str | None = None
    ethnicity: str | None = None
    if annotation != "":
        # Source data-entry errors, fixed case by case in the YAML.
        override = context.lookup_value("annotations", annotation)
        if override is not None:
            annotation = override
        match = REGEX_ANNOTATION.match(annotation)
        if match is None:
            raise ValueError(f"Unparseable annotation for {name}: {annotation!r}")
        # Annotation tokens that are neither gender nor ethnicity: the Tibet
        # delegation has two deputies both named 拉琼 (both Tibetan), which the
        # source disambiguates by home district. The token stays part of the
        # raw annotation - and thereby keeps the two entity IDs distinct - but
        # maps to no property.
        ignored_tokens = {"拉萨城关区", "拉萨达孜区"}
        for part in REGEX_ANNOTATION_SPLIT.split(match.group("parts")):
            part = part.strip()
            if part == "":
                continue
            if part == "女":
                gender = part
            elif part.endswith("族") and ethnicity is None:
                ethnicity = part
            elif part in ignored_tokens:
                continue
            else:
                raise ValueError(f"Unknown annotation token for {name}: {part!r}")

    person = context.make("Person")
    person.id = context.make_id(f"NPC-{term}", delegation, name, annotation)

    person.add("name", name, lang="zho")
    apply_translit_full_name(context, person, "chi", name, [ENGLISH])
    person.add("gender", gender)
    person.add("ethnicity", ethnicity, lang="zho")
    # NPC deputies must be PRC citizens, including those elected for the Hong
    # Kong and Macau delegations: Electoral Law of the National People's
    # Congress and Local People's Congresses, Art. 3 (electoral rights of PRC
    # citizens) and Art. 15 (election of the HK/Macau delegations),
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

    context.emit(person)
    context.emit(occupancy)


def crawl_delegation(
    context: Context,
    url: str,
    delegation: str,
    term: int,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    doc = context.fetch_html(url, cache_days=1, encoding="utf-8")
    divs = h.xpath_elements(doc, ".//div[starts-with(@class, 'md_zi')]")
    if len(divs) == 0:
        raise ValueError(
            f"No deputies listed for {delegation} ({url}) - "
            "page structure changed or a WAF challenge was served"
        )
    for div in divs:
        crawl_deputy(context, div, delegation, term, position, categorisation)
    context.log.info(f"{delegation}: {len(divs)} deputies")


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

    # The server sends no charset header; the pages declare UTF-8 in a META
    # tag only, so the encoding needs to be forced.
    stub = context.fetch_html(context.data_url, cache_days=1, encoding="utf-8")
    refresh = h.xpath_string(stub, ".//meta[@http-equiv='refresh']/@content")
    match = REGEX_REDIRECT.search(refresh)
    if match is None:
        raise ValueError(f"Cannot parse roster redirect target: {refresh!r}")
    term = int(match.group("term"))
    if term != EXPECTED_TERM:
        raise ValueError(
            f"The source now serves the roster of the {term}th NPC - review "
            "the page structure and update EXPECTED_TERM"
        )
    index_url = urljoin(context.data_url, match.group("path"))

    index = context.fetch_html(
        index_url, absolute_links=True, cache_days=1, encoding="utf-8"
    )
    # The PLA delegation's long name puts it in the wide-cell md_zi2 class.
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
            context, delegation_link, delegation_name, term, position, categorisation
        )
