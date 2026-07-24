from datetime import datetime, UTC
from typing import Any
from zoneinfo import ZoneInfo

from zavod import Context, Entity
from zavod import helpers as h


TIRANE = ZoneInfo("Europe/Tirane")


def clean_birth_date(raw: str | None) -> str | None:
    """Recover an MP's calendar birth date from the source `ditlindje` field.

    The source stores two kinds of unusable values that must be dropped: rows where
    the birth date was never entered default to the record's import timestamp (year
    2022 or 2025), and a couple carry the placeholder year 0001. Since MPs must be at
    least 18, a plausible birth year (1925–2007) cleanly separates real dates from
    these artifacts.

    Real dates are stored as local midnight expressed in UTC (e.g. a 5 July birthday
    appears as `...-07-04T23:00:00`, or `T22:00:00` in summer), so taking the date
    substring is off by one day. Converting to Europe/Tirane recovers the intended
    calendar day across both standard and daylight-saving offsets.
    """
    if raw is None:
        return None
    year = int(raw[:4])
    if year < 1925 or year > 2007:
        return None
    parsed = datetime.fromisoformat(raw).replace(tzinfo=UTC)
    return parsed.astimezone(TIRANE).date().isoformat()


def crawl_member(context: Context, position: Entity, record: dict[str, Any]) -> None:
    person = context.make("Person")
    first_name = record.pop("emer")
    last_name = record.pop("mbiemer")
    person.id = context.make_id(first_name, last_name, record.pop("id"))

    # `atesi` (patronymic) is sometimes a "-" placeholder rather than a name.
    patronymic = record.pop("atesi", None)
    if patronymic is not None and patronymic.strip("- ") == "":
        patronymic = None
    h.apply_name(
        person,
        first_name=first_name,
        patronymic=patronymic,
        last_name=last_name,
    )
    person.add("citizenship", "al")
    h.apply_date(person, "birthDate", clean_birth_date(record.pop("ditlindje")))
    person.add("birthPlace", record.pop("vendlindje", None))
    person.add("email", record.pop("email", None))
    person.add("political", record.pop("partia", None))

    occupancy = h.make_occupancy(context, person, position)
    if occupancy is None:
        return

    context.audit_data(
        record,
        ignore=[
            "qarku",  # numerical representation of the MP's electoral district
            "fotoProfil",
            "fbProfileUrl",
            "twitterProfileUrl",
            "linkedInPrifileUrl",
            "status",
            "dateKrijimi",
            "dateModifikimi",
            "krijuarNga",
            "modifikuarNga",
        ],
    )
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url)

    # the API marks sitting members with status 0
    members = [r for r in data if r.get("status") == 0]

    position = h.make_position(
        context,
        name="Member of the Parliament of Albania",
        country="al",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q20177772",
    )
    context.emit(position)

    for record in members:
        crawl_member(context, position, record)
