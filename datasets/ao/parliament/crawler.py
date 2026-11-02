import re
from collections import defaultdict
from typing import Any
from urllib.parse import urljoin

from lxml import html
from normality import squash_spaces
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.stateful.review import assert_all_accepted

from zavod import Context, Entity
from zavod import helpers as h

TOPICS = ["gov.national", "gov.legislative"]
# A short, digit-free text before a colon: the shape of a field label.
LABEL_RE = re.compile(r"^[^\d:]{3,30}$")
# A label and a value starting with a digit, without a colon between them:
# "Nascido 02/05/1962", "Data de Nascimento -01-05-1961".
LABEL_VALUE_RE = re.compile(r"^([^\d:]{3,30}?)[\s-]*(\d[^:]*)$")
# A constituency without a colon after its label, sometimes glued to the preceding
# "Gabinete Local" heading: "Gabinete LocalCírculo Eleitoral Provincial do Bié".
CONSTITUENCY_RE = re.compile(
    r"^(?:gabinete local\s*)?(c[ií]rculo eleitoral\s.+)$", re.I
)
# Stray spaces around a date separator: "18 -05- 1955".
DATE_SPACES_RE = re.compile(r"\s*([-/.])\s*")


def profile_lines(text: str) -> list[str]:
    """Split a profile (WordPress block HTML or plain text) into its text lines."""
    doc = html.fragment_fromstring(text, create_parent="div")
    # Block elements and line breaks separate the lines.
    for el in doc.iter("p", "li", "br", "div", "h1", "h2", "h3", "h4", "h5", "h6"):
        el.tail = "\n" + (el.tail or "")
    lines = [" ".join(line.split()) for line in doc.text_content().split("\n")]
    return [line for line in lines if line]


def parse_profile(context: Context, text: str) -> dict[str, list[str]]:
    """Group the lines of a profile by the field their label introduces.

    A label is followed by a colon and the value, directly by a value starting with
    a digit, or stands alone with the value on the following line. Each in-scope field takes one value; any other
    label-shaped line ends the field."""
    fields: dict[str, list[str]] = defaultdict(list)
    field: str | None = None
    for line in profile_lines(text):
        label, colon, value = line.partition(":")
        match = CONSTITUENCY_RE.match(line) if not colon else None
        if match is not None:
            fields["constituency"].append(match.group(1))
            field = None
            continue
        res = context.lookup("profile_label", label) if colon else None
        split = LABEL_VALUE_RE.match(line) if not colon else None
        if split is not None:
            res = context.lookup("profile_label", split.group(1))
            value = split.group(2)
        if res is None:
            res = context.lookup("profile_label", line)
            value = ""
        if res is not None:
            field = res.value
            line = value.strip()
        elif colon and LABEL_RE.match(label):
            field = None
        if field is None or line == "":
            continue
        fields[field].append(line)
        field = None
    return fields


def clean_date(value: str) -> str:
    """Normalise typing noise so the `dates.formats` apply: '09 – 09 - 1958,'."""
    value = DATE_SPACES_RE.sub(r"\1", value.replace("–", "-"))
    return value.rstrip(".,:")


def make_person(context: Context, name: str, title: str) -> Entity:
    person = context.make("Person")
    person.id = context.make_id(name)
    names = h.Names(name=name)
    # The display title is sometimes a shorter or differently spelled form.
    if squash_spaces(title).lower() != squash_spaces(name).lower():
        names.add("alias", title)
    h.apply_reviewed_names(context, person, original=names, llm_cleaning=True)
    # Deputies must be Angolan citizens: Lei n.º 36/11 (Lei Orgânica sobre as Eleições
    # Gerais), art. 14. https://lex.ao/docs/assembleia-nacional/2011/lei-n-o-36-11-de-21-de-dezembro/
    person.add("citizenship", "ao")
    return person


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    legislature: str,
    is_latest: bool,
    record: dict[str, Any],
) -> None:
    name = record.pop("keyMenu")
    succession = context.lookup("succession", name)
    if succession is not None:
        name = succession.member
    elif "(" in name:
        context.log.warning("Unknown succession", name=name)
        return
    fields = parse_profile(context, record.pop("context"))

    person = make_person(context, name, record.pop("title"))
    for value in fields["birth_date"]:
        h.apply_date(person, "birthDate", clean_date(value), original_value=value)
    person.add("birthPlace", fields["birth_place"])
    for value in fields["party"]:
        person.add(
            "political", context.lookup_value("party", value, warn_unmatched=True)
        )

    start_date = None
    # Some records carry a date of birth or another legislature's date here.
    if context.lookup("invalid_sworn_in", name) is None:
        if len(fields["sworn_in"]) > 1:
            context.log.warning(
                "Several sworn-in dates", name=name, values=fields["sworn_in"]
            )
        for value in fields["sworn_in"]:
            start_date = clean_date(value)
    end_date = None
    for value in fields["mandate_lost"]:
        end_date = clean_date(value)
    context.audit_data(
        record,
        ignore=[
            "id",
            "imgCapa",
            "email",
            "object_iuu",
            "user_id",
            "ordem",
            "created_at",
            "updated_at",
            "deleted_at",
            "localkeyMenu",
            "entityType",
        ],
    )

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=end_date,
        no_end_implies_current=is_latest,
        key_prefix=legislature,
    )
    if occupancy is None:
        return
    for value in fields["constituency"]:
        res = context.lookup("constituency", value, warn_unmatched=True)
        if res is not None:
            occupancy.add("constituency", res.value)
    for value in fields["political_group"]:
        occupancy.add(
            "politicalGroup",
            context.lookup_value("party", value, warn_unmatched=True),
        )
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Angola",
        wikidata_id="Q20179557",
        country="ao",
        topics=TOPICS,
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # The "Deputados" menu has one entry per legislature, each with a "Perfil" page
    # listing its members. The source gives no term dates: the roster marked
    # `current` in the `legislature` lookup is the sitting one, earlier rosters
    # resolve to an unknown status. A new legislature fails the required lookup.
    menu = context.fetch_json(context.data_url)
    deputies_menu = [m for m in menu["data"]["mainMenu"] if m["context"] == "Deputados"]
    assert len(deputies_menu) == 1, deputies_menu
    for item in deputies_menu[0]["elements"]:
        legislature = item["context"]
        term = context.lookup("legislature", legislature)
        assert term is not None, legislature
        profiles = [
            e["url"] for e in item["elements"] if e["context"].startswith("Perfil")
        ]
        assert len(profiles) == 1 and profiles[0] in term.lists, (term, profiles)
        for list_key in term.lists:
            data = context.fetch_json(urljoin(context.data_url, list_key))
            for record in data["data"]:
                crawl_member(
                    context,
                    position,
                    categorisation,
                    legislature,
                    term.current is True,
                    record,
                )

    assert_all_accepted(context, raise_on_unaccepted=False)
