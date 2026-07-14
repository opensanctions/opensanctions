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
# 22 provinces, 5 autonomous regions, 4 municipalities, PLA and armed police,
# Hong Kong, Macau, Taiwan.
EXPECTED_DELEGATIONS = 35


def crawl_deputy(
    context: Context,
    div: Element,
    delegation: str,
    term: int,
    position: Entity,
    categorisation: PositionCategorisation,
    seen: set[str],
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
    # The source has no per-deputy identifiers, so identity is the listing
    # itself. The term is part of the key so that a same-named deputy in a
    # future congress cannot silently inherit this entity; cross-term
    # continuity of re-elected deputies is left to downstream deduplication.
    person_id = context.make_id(f"NPC-{term}", delegation, name, annotation)
    if person_id is None:
        raise ValueError(f"Could not build an ID for {name} in {delegation}")
    if person_id in seen:
        raise ValueError(
            f"Indistinguishable deputies: {name} {annotation!r} in {delegation}"
        )
    seen.add(person_id)
    person.id = person_id

    person.add("name", name, lang="zho")
    apply_translit_full_name(context, person, "chi", name, [ENGLISH])
    # Only women are annotated; the source's silence on the rest is not
    # interpreted as "male".
    person.add("gender", gender)
    # Only the 55 recognized minorities are annotated; Han is never stated
    # and therefore not inferred.
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
        # The roster is maintained: deputies whose credentials are terminated
        # are removed from the page, so being listed means currently in
        # office. The source states no start or end dates.
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        raise ValueError(f"Occupancy not created for {name}")
    # The delegation (代表团) is the unit the deputy is elected to represent.
    # It's usually a province/region, but includes the military (解放军和武警
    # 部队), so stuffing it into constituency - defined as a geographic area -
    # is somewhat of a misuse of the field.
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
    seen: set[str],
) -> None:
    doc = context.fetch_html(url, cache_days=1, encoding="utf-8")
    divs = h.xpath_elements(doc, ".//div[starts-with(@class, 'md_zi')]")
    if len(divs) == 0:
        raise ValueError(
            f"No deputies listed for {delegation} ({url}) - "
            "page structure changed or a WAF challenge was served"
        )
    for div in divs:
        crawl_deputy(context, div, delegation, term, position, categorisation, seen)
    context.log.info(f"{delegation}: {len(divs)} deputies")


def crawl(context: Context) -> None:
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

    position = h.make_position(
        context,
        "Member of the National People's Congress",
        country="cn",
        # "National People's Congress deputy" - term-agnostic
        wikidata_id="Q10891456",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    index = context.fetch_html(
        index_url, absolute_links=True, cache_days=1, encoding="utf-8"
    )
    # The PLA delegation's long name puts it in the wide-cell md_zi2 class.
    links = h.xpath_elements(
        index,
        ".//div[starts-with(@class, 'md_zi')]/a",
        expect_exactly=EXPECTED_DELEGATIONS,
    )
    seen: set[str] = set()
    for link in links:
        delegation = h.element_text(link)
        href = link.get("href")
        if href is None or delegation == "":
            raise ValueError(f"Delegation link without target or label: {delegation!r}")
        crawl_delegation(
            context, href, delegation, term, position, categorisation, seen
        )
