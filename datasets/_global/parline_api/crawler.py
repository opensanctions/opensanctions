from zavod import Context, helpers as h
from zavod.stateful.positions import OccupancyStatus, categorise


def parse_dob(item):
    dob_year = item.pop("dob_year", None)
    dob_month = item.pop("dob_month", None)
    dob_day = item.pop("dob_day", None)
    dob = None
    if dob_year is not None:
        # Start with the year
        dob = f"{dob_year:04d}"
        # Add month if available
        if dob_month is not None:
            dob += f"-{dob_month:02d}"
            # Add day if available
            if dob_day is not None:
                dob += f"-{dob_day:02d}"
    return dob


def crawl_item(context: Context, item: dict):
    person_id = item.pop("person_code")
    dob = parse_dob(item)
    first_name = item.pop("first_name")
    last_name = item.pop("family_name")

    if not first_name or not last_name:
        return
    pep = context.make("Person")
    pep.id = context.make_id(person_id)
    h.apply_name(pep, first_name=first_name, last_name=last_name, lang="eng")
    h.apply_date(pep, "birthDate", dob)
    pep.add("country", item.pop("person_country"))
    pep.add("title", item.pop("title_salutation"))
    pep.add("topics", ["gov.national", "gov.legislative", "role.pep"])
    position = h.make_position(
        context, name="Member of Parliament", country=pep.get("country")
    )
    categorisation = categorise(context, position, True)
    occupancy = h.make_occupancy(
        context,
        pep,
        position,
        no_end_implies_current=False,
        status=OccupancyStatus.UNKNOWN,
        categorisation=categorisation,
    )
    if occupancy:
        context.emit(pep)
        context.emit(occupancy)
        context.emit(position)
        context.audit_data(item, ["gender", "updated_by"])


def crawl(context: Context):
    url = context.data_url
    while url:
        response = context.fetch_json(url, cache_days=1)
        data = response.get("data", [])
        links = response.get("links", {})
        next_url = links.get("next")
        for item in data:
            crawl_item(context, item)
        if url == next_url:
            break
        url = next_url
