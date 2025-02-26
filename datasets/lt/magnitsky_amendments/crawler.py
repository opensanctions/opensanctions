import json
from zavod import Context
from zavod import helpers as h


def map_gender(gender_code):
    gender_map = {
        "V": "male",
        "M": "female",  # Assuming "M" stands for female in Lithuanian
    }
    return gender_map.get(gender_code)


def crawl_page(context: Context, entry):
    person = context.make("Person")
    # Create an ID from the person's first name, last name, and date of birth
    person_id = context.make_id(
        entry.get("vardas"), entry.get("pavarde"), entry.get("gimimoData")
    )
    person.id = person_id
    person.add("name", f"{entry.get('vardas')} {entry.get('pavarde')}", lang="lit")
    h.apply_date(person, "birthDate", entry.get("gimimoData"))
    person.add("topics", "sanction")
    gender = map_gender(entry.get("lytis"))
    if gender:
        person.add("gender", gender)

    # Add citizenships if available
    citizenships = entry.get("pilietybes", [])
    for citizenship in citizenships:
        code = citizenship.get("valstybe", {}).get("pilietybeClaEntry", {}).get("key")
        if code:
            person.add("citizenship", code, lang="lit")

    # Create a Sanction entity
    sanction = h.make_sanction(context, person)
    sanction.add("reason", entry.get("priezastis"), lang="lit")
    for date in h.multi_split(entry.get("priezastis"), ["galiojanti nuo "]):
        h.apply_date(sanction, "startDate", date)
    h.apply_date(sanction, "endDate", entry.get("uzdraustaIki"))

    # Emit entities
    context.emit(person)
    context.emit(sanction)


def crawl(context: Context):
    page = 0
    while True:
        url = f"https://www.migracija.lt/external/nam/search?pageNo={page}&pageSize=100&language=lt"
        path = context.fetch_resource(f"source_{page}.json", url)
        context.export_resource(
            path, mime_type="application/json", title=f"page_{page}"
        )
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            if data.get("list") == []:
                break
        page += 1
        # Limit the crawl to a maximum of 50 pages to prevent infinite loops
        if page > 50:
            context.log.warning("Suspicions of infinite loop")
            break

        for entry in data.get("list"):
            crawl_page(context, entry)
