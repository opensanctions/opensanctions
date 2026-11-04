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

# A label directly followed by a date, without a colon: "Nascido 02/05/1962".
LABEL_DATE_RE = re.compile(r"^([^\d:]{3,30}?)[\s-]*(\d[^:]*)$")
# Stray spaces around a date separator: "18 -05- 1955".
DATE_SPACES_RE = re.compile(r"\s*([-/.])\s*")


def parse_profile(context: Context, text: str) -> dict[str, list[str]]:
    """Map the labelled lines of a profile (WordPress block HTML) to fields.

    A label is followed by a colon and the value, directly by a date, or stands alone
    with the value on the next line. Labels missing from the `profile_label` lookup
    (education, committees, career) are out of scope."""
    doc = html.fragment_fromstring(text, create_parent="div")
    # Block elements and line breaks separate the lines.
    for el in doc.iter("p", "li", "br", "div", "h1", "h2", "h3", "h4", "h5", "h6"):
        el.tail = "\n" + (el.tail or "")
    fields: dict[str, list[str]] = defaultdict(list)
    # A label whose value is on the next line.
    field: str | None = None
    for line in doc.text_content().split("\n"):
        line = squash_spaces(line)
        if line == "":
            continue
        label, colon, value = line.partition(":")
        if colon == "":
            match = LABEL_DATE_RE.match(line)
            label, value = match.groups() if match is not None else (line, "")
        labelled = context.lookup_value("profile_label", label)
        if labelled is not None:
            field, value = labelled, value.strip()
        elif colon != "" or field is None:
            # A line with a label of its own ends the field.
            field = None
            continue
        else:
            value = line
        if value != "":
            fields[field].append(value)
            field = None
    return fields


def clean_date(value: str) -> str:
    """Normalise typing noise so the `dates.formats` apply: '09 – 09 - 1958,'."""
    value = DATE_SPACES_RE.sub(r"\1", value.replace("–", "-"))
    return value.rstrip(".,:")


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    legislature: str,
    is_current: bool,
    record: dict[str, Any],
) -> None:
    name = record.pop("keyMenu")
    succession = context.lookup("succession", name)
    if succession is not None:
        name = succession.member
    elif "(" in name:
        context.log.warning("Unknown succession", name=name)
        return
    title = record.pop("title")
    fields = parse_profile(context, record.pop("context"))

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
    for value in fields["birth_date"]:
        h.apply_date(person, "birthDate", clean_date(value), original_value=value)
    person.add("birthPlace", fields["birth_place"])
    for value in fields["party"]:
        res = context.lookup("party", value)
        person.add("political", value if res is None else res.value)

    start_date = None
    # Some records carry a date of birth or another legislature's date here.
    if context.lookup("invalid_sworn_in", name) is None:
        if len(fields["sworn_in"]) > 1:
            context.log.warning(
                "Several sworn-in dates", name=name, values=fields["sworn_in"]
            )
        for value in fields["sworn_in"]:
            start_date = clean_date(value)
    end_dates = fields["mandate_lost"]
    assert len(end_dates) <= 1, (name, end_dates)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=clean_date(end_dates[0]) if end_dates else None,
        no_end_implies_current=is_current,
        key_prefix=legislature,
    )
    if occupancy is None:
        return
    for value in fields["constituency"]:
        res = context.lookup("constituency", value)
        occupancy.add("constituency", value if res is None else res.value)
    # The parliamentary group, distinct from party membership.
    occupancy.add("politicalGroup", fields["political_group"])
    context.emit(occupancy)
    context.emit(person)

    context.audit_data(
        record,
        ignore=[
            "id",
            "imgCapa",
            # Contact details are not extracted for PEPs.
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


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Angola",
        wikidata_id="Q20179557",
        country="ao",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # The menu lists the legislatures, sitting one first. It is pinned so that a new
    # one fails the crawl; earlier ones have no term dates and resolve to unknown.
    menu = context.fetch_json(context.data_url)
    deputies = [m for m in menu["data"]["mainMenu"] if m["context"] == "Deputados"]
    assert len(deputies) == 1, deputies
    legislatures = deputies[0]["elements"]
    assert legislatures[0]["context"] == "V Legislatura", legislatures[0]["context"]
    for index, legislature in enumerate(legislatures):
        profiles = [
            e["url"]
            for e in legislature["elements"]
            if e["context"].startswith("Perfil")
        ]
        assert len(profiles) == 1, profiles
        data = context.fetch_json(urljoin(context.data_url, profiles[0]))
        for record in data["data"]:
            crawl_member(
                context,
                position,
                categorisation,
                legislature["context"],
                index == 0,
                record,
            )

    assert_all_accepted(context, raise_on_unaccepted=False)
