from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

# The senator directory is populated by this AJAX endpoint; session 5 is the current
# (5th) legislature.
SESSION = "5"
HEADERS = {
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://senate.gov.kh/search-senator-all/",
}


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Cambodia",
        country="kh",
        wikidata_id="Q21295127",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    records: list[dict[str, Any]] = context.fetch_json(
        context.data_url,
        method="POST",
        headers=HEADERS,
        data={"session": SESSION, "keyword": "", "chck_status": "1"},
        cache_days=1,
    )
    if not records:
        raise ValueError("Senate AJAX endpoint returned no senators")

    for record in records:
        str_id = record.pop("str_id")
        name = record.pop("name")
        assert name, f"Empty name for senator {str_id}"

        person = context.make("Person")
        person.id = context.make_slug(str_id)
        person.add("name", name, lang="khm")
        person.add("gender", record.pop("gender", None))
        h.apply_date(person, "birthDate", record.pop("dob", None))
        person.add("political", record.pop("party", None), lang="khm")
        person.add("sourceUrl", record.pop("biography", None))
        # Senators must be Khmer citizens (Constitution of Cambodia, Article 34 (New)).
        # https://constitutionnet.org/sites/default/files/Cambodia%20Constitution.pdf
        person.add("citizenship", "kh")

        # Some records carry a data-entry error where the start date is the senator's
        # birth year (e.g. "1950-07-06"); ignore any start before the modern Senate.
        start_date = record.pop("start", None)
        if (
            start_date is not None
            and start_date[:4].isdigit()
            and int(start_date[:4]) < 2000
        ):
            start_date = None
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=start_date,
            end_date=record.pop("end", None),
            categorisation=categorisation,
        )
        if occupancy is None:
            continue

        context.audit_data(
            record,
            ignore=[
                "photo",
                "phone",
                "status",
                "replaceby",
                "session",
                "session_id",
            ],
        )
        context.emit(occupancy)
        context.emit(person)
