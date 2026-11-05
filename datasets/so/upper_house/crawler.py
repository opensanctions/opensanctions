import json
import re
from typing import cast

from lxml.html import HtmlElement

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.util import Element


MEMBER = "Member of the Upper House of Somalia"
POSITION_QIDS = {
    MEMBER: "Q21328640",
    "Speaker of the Upper House of Somalia": "Q109745150",
}
JS_COMMENT = re.compile(r"//[^\n]*")
JS_KEY = re.compile(r"([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:")
JS_DANGLING_COMMA = re.compile(r",(\s*[}\]])")
SLASH_DATE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def parse_roster(script: str) -> list[dict[str, str]]:
    """Parse the `const senators` array literal the page holds its members in.

    The literal is JSON apart from unquoted keys, `//` comments and a dangling
    comma, and its strings carry no backslash escapes — so splitting on the quote
    character isolates every string body in an odd-numbered chunk, and the three
    differences can be rewritten in the even-numbered chunks without touching a
    value. The assertion guards the one assumption that makes that split valid.
    """
    start = script.index("const senators = [")
    body = script[script.index("[", start) : script.index("];", start) + 1]
    assert "\\" not in body, "Escaped character in roster literal"
    chunks = body.split('"')
    for index in range(0, len(chunks), 2):
        clean = JS_DANGLING_COMMA.sub(r"\1", JS_COMMENT.sub("", chunks[index]))
        chunks[index] = JS_KEY.sub(r'\1"\2":', clean)
    members = json.loads('"'.join(chunks), strict=False)
    assert isinstance(members, list), type(members)
    for member in members:
        assert isinstance(member, dict), member
        for key, value in member.items():
            assert isinstance(key, str) and isinstance(value, str), member
    return members


def read_members(doc: Element) -> list[dict[str, str]]:
    """Read the whole chamber off the page, presiding officers first.

    The directory is a hand-written HTML block rather than rendered markup, so the
    two kinds of record are stored differently: the presiding officers as `data-`
    attributes on their cards, the ordinary senators only inside a script. Both
    carry the same fields, and only the officers' cards name a role.
    """
    cards = [
        # Cast because lxml-stubs types a plain element's attribute names as
        # `str | bytes`, as zavod.helpers.html does for the same reason.
        {
            k[5:]: v
            for k, v in cast(HtmlElement, card).attrib.items()
            if k.startswith("data-")
        }
        for card in h.xpath_elements(doc, "//button[contains(@class, 'view-btn')]")
    ]
    roles = h.xpath_strings(
        doc,
        "//div[contains(@class, 'leader-role') or contains(@class, 'whip-role')]/text()",
    )
    for card, role in zip(cards, roles, strict=True):
        card["role"] = role.strip()
    script = h.xpath_string(doc, "//script[contains(text(), 'const senators')]/text()")
    return cards + parse_roster(script)


def apply_birth_date(person: Entity, value: str) -> None:
    """Apply a date of birth, keeping only the year where its order is ambiguous.

    Most slash-separated dates are written day first, but at least two
    (`10/21/1969`, `5/15/1951`) are written month first, so such a date only
    determines a day and a month where one of the two readings is impossible.
    Where both readings work, the year is all the source actually pins down.
    """
    match = SLASH_DATE.match(value)
    if match is None:
        h.apply_date(person, "birthDate", value)
        return
    first, second, year = int(match.group(1)), int(match.group(2)), match.group(3)
    readings = {(d, m) for d, m in ((first, second), (second, first)) if m <= 12}
    if len(readings) != 1:
        h.apply_date(person, "birthDate", year)
        return
    day, month = readings.pop()
    h.apply_date(person, "birthDate", f"{year}-{month:02d}-{day:02d}")


def crawl_member(
    context: Context, member: dict[str, str], translated: dict[str, str]
) -> None:
    role = member.pop("role", None)
    somali_name = member.pop("name")

    person = context.make("Person")
    person.id = context.make_id(somali_name)
    # FollowTheMoney's language list does not extend to Somali, so only the names
    # from the English rendering can be tagged with the language they are in.
    for raw, lang in ((somali_name, None), (translated["name"], "eng")):
        name = h.strip_name_titles(context, raw)
        person.add("name", name, lang=lang, original_value=raw if name != raw else None)
        # Several senators are commonly known by the nickname their name carries
        # in brackets, so keep the plain form alongside the full one.
        person.add("name", h.remove_bracketed(name), lang=lang)
    apply_birth_date(person, member.pop("dob"))
    # Members of either chamber of the Federal Parliament must be Somali citizens:
    # Provisional Constitution of the Federal Republic of Somalia (2012), Art.
    # 58(1)(a), "Membership Criteria for the Federal Parliament" — Art. 55 defines
    # that Parliament as the House of the People and the Upper House together, and
    # Chapter 6 states no separate eligibility criteria for the Upper House.
    # https://www.constituteproject.org/constitution/Somalia_2012
    person.add("citizenship", "so")

    # The source names the federal member state a senator represents rather than an
    # electoral district: the seats are apportioned between the states and filled by
    # their electoral colleges.
    constituency = translated["region"]
    if role is None:
        names = [MEMBER]
    else:
        result = context.lookup("position", role)
        assert result is not None, role
        names = result.values

    for name in names:
        position = h.make_position(
            context,
            name=name,
            country="so",
            topics=["gov.national", "gov.legislative"],
            wikidata_id=POSITION_QIDS.get(name),
            lang="eng",
        )
        categorisation = categorise(context, position)
        if not categorisation.is_pep:
            continue
        # The roster is maintained — an outgoing chamber is moved to a separate list
        # of former senators — but gives no date on which anyone entered office, so
        # an occupancy is undated and current.
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        occupancy.add("constituency", constituency)
        context.emit(occupancy)
        context.emit(position)
        context.emit(person)

    # The English rendering carries the same fields; only the name and the federal
    # member state are taken from it.
    context.audit_data(
        member,
        ignore=[
            # The member state, taken from the English rendering instead.
            "region",
            "img",
            # Empty on every record, and contact details are not extracted for PEPs.
            "email",
            # A free-text curriculum vitae, with no FollowTheMoney property to hold
            # it. It names the year a senator entered office too inconsistently to
            # date an occupancy from: a third of the records leave it empty, and
            # some lead with a ministerial post rather than the seat.
            "education",
            "jobs",
            "trainings",
            "workshops",
            "language",
            "publications",
        ],
    )


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    # The same page in English. Its names are transliterated independently of the
    # Somali ones ("Cabdullaahi Sheekh Ismaaciil" / "Abdullahi Sheikh Ismail"), so
    # both spellings are worth holding, and it names the federal member states in
    # English. It renders the same records in the same order.
    translated_doc = context.fetch_html(context.data_url, params={"lang": "en"})

    for member, translated in zip(
        read_members(doc), read_members(translated_doc), strict=True
    ):
        # The untranslated fields pair the two renderings up. A mismatch means they
        # have drifted apart and the English names would land on the wrong people.
        assert member["dob"] == translated["dob"], (member, translated)
        assert member.get("img") == translated.get("img"), (member, translated)
        crawl_member(context, member, translated)
