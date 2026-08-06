from datetime import datetime

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Regional Representative Council of Indonesia",
        country="id",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328635",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    members = zyte_api.fetch_json(
        # The API host sits behind a WAF that rejects ordinary egress
        context,
        context.data_url,
        geolocation="id",
        cache_days=7,
    )
    for member in members:
        name = member.pop("fullName")
        dob = member.pop("dateOfBirth")

        person = context.make("Person")
        person.id = context.make_id(name, dob)
        person.add("name", name)
        person.add("gender", member.pop("gender"))
        person.add("birthPlace", member.pop("placeOfBirth"))
        h.apply_date(person, "birthDate", dob)
        person.add("biography", member.pop("profile"))  # local lang
        person.add("email", member.pop("email"))
        # DPD candidates must be Indonesian citizens (Law No. 7 of 2017 on General
        # Elections, Article 182 letter a). https://peraturan.bpk.go.id/Details/37644
        person.add("citizenship", "id")

        periods = member.pop("memberPeriods") or []
        province = None
        start_date = None

        if periods:
            if len(periods) == 1:
                latest = periods[0]
            else:
                latest = max(
                    periods, key=lambda p: datetime.fromisoformat(p["inaugurationDate"])
                )
            province = latest["region"]["name"]
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
