import json
import re
from typing import cast

from lxml.html import HtmlElement

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)
from zavod.util import Element


JS_COMMENT = re.compile(r"//[^\n]*")
JS_KEY = re.compile(r"([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:")
JS_DANGLING_COMMA = re.compile(r",(\s*[}\]])")
SLASH_DATE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def parse_roster(doc: Element, variable: str) -> list[dict[str, str]]:
    """Parse a `const <variable> = [...]` array literal the page holds senators in.

    The literal is JSON apart from unquoted keys, `//` comments and a dangling
    comma, and its strings carry no backslash escapes — so splitting on the quote
    character isolates every string body in an odd-numbered chunk, and the three
    differences can be rewritten in the even-numbered chunks without touching a
    value. The assertion guards the one assumption that makes that split valid.
    """
    declaration = f"const {variable} = ["
    script = h.xpath_string(doc, f"//script[contains(text(), '{declaration}')]/text()")
    start = script.index(declaration)
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
    """Read the sitting chamber off the page, presiding officers first.

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
    return cards + parse_roster(doc, "senators")


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
    context: Context,
    membership: Entity,
    categorisation: PositionCategorisation,
    member: dict[str, str],
    translated: dict[str, str],
    role: str | None,
    dob: str | None,
    status: OccupancyStatus,
) -> None:
    """Emit one senator, with their seat and any presiding office they hold.

    `status` is CURRENT for a sitting senator and ENDED for a former one: the page
    separates the two, but dates neither.
    """
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
    if dob is not None:
        apply_birth_date(person, dob)
    # Members of either chamber of the Federal Parliament must be Somali citizens:
    # Provisional Constitution of the Federal Republic of Somalia (2012), Art.
    # 58(1)(a), "Membership Criteria for the Federal Parliament" — Art. 55 defines
    # that Parliament as the House of the People and the Upper House together, and
    # Chapter 6 states no separate eligibility criteria for the Upper House.
    # https://www.constituteproject.org/constitution/Somalia_2012
    person.add("citizenship", "so")

    positions = [(membership, categorisation)]
    if role is not None:
        res = context.lookup("position", role)
        assert res is not None, role
        if res.name is not None:
            office = h.make_position(
                context,
                name=res.name,
                country="so",
                topics=res.topics,
                wikidata_id=res.wikidata_id,
                lang="eng",
            )
            positions.append((office, categorise(context, office)))

    # The source names the federal member state a senator represents rather than an
    # electoral district: the seats are apportioned between the states and filled by
    # their state assemblies.
    constituency = translated["region"]
    for position, position_categorisation in positions:
        if not position_categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=position_categorisation,
            status=status,
        )
        # With the status given, only a position that is not a PEP yields None.
        assert occupancy is not None, position.id
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

    membership = h.make_position(
        context,
        name="Member of the Upper House of Somalia",
        country="so",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328640",
        lang="eng",
    )
    categorisation = categorise(context, membership)
    if not categorisation.is_pep:
        return

    for member, translated in zip(
        read_members(doc), read_members(translated_doc), strict=True
    ):
        # The untranslated fields pair the two renderings up. A mismatch means they
        # have drifted apart and the English names would land on the wrong people.
        assert member["dob"] == translated["dob"], (member, translated)
        assert member.get("img") == translated.get("img"), (member, translated)
        role = member.pop("role", None)
        dob = member.pop("dob")
        crawl_member(
            context,
            membership,
            categorisation,
            member,
            translated,
            role,
            dob,
            OccupancyStatus.CURRENT,
        )

    # The page renders these as "former-senator-card" cards: senators of the previous
    # chamber, who no longer hold the seat. Their records carry no dates, so their
    # occupancies are ENDED, which never age out. The page lists only the one previous
    # chamber, whose members left office when the sitting one was seated: within the
    # 20-year after-office window for national positions.
    for member, translated in zip(
        parse_roster(doc, "formerSenators"),
        parse_roster(translated_doc, "formerSenators"),
        strict=True,
    ):
        assert member["img"] == translated["img"], (member, translated)
        role = member.pop("reason")
        crawl_member(
            context,
            membership,
            categorisation,
            member,
            translated,
            role,
            None,
            OccupancyStatus.ENDED,
        )
