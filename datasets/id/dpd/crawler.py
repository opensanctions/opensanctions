from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

# The API host sits behind a WAF that rejects ordinary egress, so it is fetched through the
# Zyte API with an Indonesian exit.
GEOLOCATION = "id"


def member_list(payload: Any) -> list[dict[str, Any]]:
    """Return the member array from the API envelope.

    The service wraps the rows in a `data` object; be tolerant of a bare list or one extra
    level of nesting so a minor envelope change fails loudly rather than silently.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            return list(data["data"])
    raise ValueError("Unexpected DPD API response shape")


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Regional Representative Council of Indonesia",
        country="id",
        wikidata_id="Q21328635",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    payload = zyte_api.fetch_json(
        context, context.data_url, geolocation=GEOLOCATION, cache_days=1
    )
    members = member_list(payload)
    if not members:
        raise ValueError("DPD API returned no members")

    for member in members:
        name = member.get("fullName")
        assert name, f"Member without a name: {member!r}"

        person = context.make("Person")
        person.id = context.make_id(name, member.get("dateOfBirth"))
        person.add("name", name)
        person.add("gender", member.get("gender"))
        person.add("birthPlace", member.get("placeOfBirth"))
        h.apply_date(person, "birthDate", member.get("dateOfBirth"))
        person.add("email", member.get("email"))
        # DPD candidates must be Indonesian citizens (Law No. 7 of 2017 on General
        # Elections, Article 182 letter a). https://peraturan.bpk.go.id/Details/37644
        person.add("citizenship", "id")

        # The most recent member period gives the province and inauguration date.
        periods = member.get("memberPeriods") or []
        province = None
        start_date = None
        if periods:
            latest = periods[-1]
            region = latest.get("region") or {}
            province = region.get("name")
            start_date = latest.get("inaugurationDate")

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=start_date,
            categorisation=categorisation,
        )
        if occupancy is None:
            continue
        if province is not None:
            occupancy.add("constituency", province)
        context.emit(occupancy)
        context.emit(person)
