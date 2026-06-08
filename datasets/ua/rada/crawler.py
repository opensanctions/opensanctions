from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The data URL is pinned to the 9th convocation ("skl9"). The Verkhovna Rada
# publishes a separate feed per convocation; when a new convocation is elected,
# a new feed (e.g. ".../skl10/mps10-data.json") is published and this one stops
# being updated. The freshness check below fails loudly so the URL/convocation
# is reviewed and bumped rather than the crawler silently emitting stale data.
EXPECTED_CONVOCATION = 9

# Ukrainian: 1 = male, 2 = female (mapped to FtM via the type.gender lookup).


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, Any],
) -> None:
    """Build and emit a single active People's Deputy."""
    member_id = row.pop("id")
    if member_id is None:
        raise ValueError("Member record without an id")

    convocation = row.pop("convocation")
    if convocation != EXPECTED_CONVOCATION:
        raise ValueError(
            f"Unexpected convocation {convocation!r} for member {member_id!r}; "
            f"expected {EXPECTED_CONVOCATION}. The source feed may have rolled "
            "over to a new convocation - review the data URL."
        )

    person = context.make("Person")
    person.id = context.make_slug("mp", member_id)

    full_name = row.pop("full_name")
    if full_name is None:
        raise ValueError(f"Member {member_id!r} has no full_name")

    # Names are in Ukrainian Cyrillic. The source splits them into family name,
    # given name and patronymic (father-derived name), plus a precomposed full
    # name. data.lang (ukr) is the default; set it explicitly here for clarity.
    h.apply_name(
        person,
        full=full_name,
        first_name=row.pop("first_name"),
        last_name=row.pop("last_name"),
        patronymic=row.pop("second_name"),
        lang="ukr",
    )
    # A small number of records carry an additional/previous full name form.
    other_name = row.pop("other_name", None)
    if other_name is not None:
        h.apply_name(person, full=other_name, lang="ukr", alias=True)

    h.apply_date(person, "birthDate", row.pop("birthday"))
    person.add("gender", row.pop("gender"))
    person.add("political", row.pop("party_name"))
    person.add("political", row.pop("party_text"))
    person.add("sourceUrl", row.pop("anketa_url"))
    person.add("notes", row.pop("anketa_data"), lang="ukr")

    # The Constitution of Ukraine, Art. 76, requires a People's Deputy to be a
    # citizen of Ukraine. https://zakon.rada.gov.ua/laws/show/254%D0%BA/96-%D0%B2%D1%80#Text
    person.add("citizenship", "ua")

    # IMPORTANT: set ALL person props BEFORE make_occupancy - it reads
    # birthDate/citizenship to determine PEP status.
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        # date_begin/date_end are ISO YYYY-MM-DD in the source.
        # TODO: confirm date_begin/date_end formats against a live run.
        start_date=row.pop("date_begin"),
        end_date=row.pop("date_end", None),
        no_end_implies_current=True,
        propagate_country=True,
    )
    if occupancy is not None:
        context.emit(occupancy)
        # IMPORTANT: emit person AFTER make_occupancy - it adds role.pep.
        context.emit(person)

    # Fields not modelled: photos, district/region, education and biographical
    # detail (covered by notes), party identifiers, and presence/old-mandate
    # bookkeeping.
    context.audit_data(
        row,
        ignore=[
            "rada_id",
            "party_id",
            "photo_id",
            "photo",
            "region_id",
            "region_name",
            "district_name",
            "district_num",
            "district_text",
            "education",
            "education_old",
            "academic",
            "college",
            "old_post",
            "old_profession",
            "old_member",
            "new_member",
            "num_in_party",
            "nreg",
            "short_name",
            "resignation_text",
            "presentAuto_absent",
        ],
    )


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url)
    if not isinstance(data, list):
        raise ValueError("Expected a JSON list of members")

    # Freshness check: the feed is convocation-bound. If the active count drops
    # far below a full chamber, or the convocation rolls over, fail loudly.
    active = [row for row in data if not row.get("date_end")]
    if len(active) < 300:
        raise ValueError(
            f"Only {len(active)} active deputies found (expected ~450). "
            "The source feed may have rolled over to a new convocation - "
            "review the data URL and the EXPECTED_CONVOCATION constant."
        )

    position = h.make_position(
        context,
        name="Member of the Verkhovna Rada of Ukraine",
        country="ua",
        wikidata_id="Q12132454",
    )
    categorisation = categorise(context, position, is_pep=True)
    context.emit(position)

    for row in active:
        crawl_member(context, position, categorisation, row)
