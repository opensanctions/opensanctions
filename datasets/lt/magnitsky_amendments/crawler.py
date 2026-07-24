import json
from typing import Any

from zavod import Context
from zavod import helpers as h


def map_gender(gender_code: str) -> str | None:
    gender_map = {
        "V": "male",
        "M": "female",  # Assuming "M" stands for female in Lithuanian
    }
    return gender_map.get(gender_code)


def crawl_page(context: Context, entry: dict[str, Any]) -> None:
    person = context.make("Person")
    # ID based on the person's first and last name
    person_id = context.make_id(entry.get("vardas"), entry.get("pavarde"))
    person.id = person_id
    person.add("name", f"{entry.get('vardas')} {entry.get('pavarde')}", lang="lit")
    h.apply_date(person, "birthDate", entry.get("gimimoData"))
    person.add("topics", "sanction")
    gender_raw = entry.get("lytis")
    gender = map_gender(gender_raw) if isinstance(gender_raw, str) else None
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


def crawl(context: Context) -> None:
    page = 0
    while True:
        url = f"https://www.migracija.lt/external/nam/search?pageNo={page}&pageSize=100&language=lt"
        path = context.fetch_resource(f"source_{page}.json", url)
        context.export_resource(
            path, mime_type="application/json", title=f"page_{page}"
        )
        with open(path, encoding="utf-8") as fh:
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
