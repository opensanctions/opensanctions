import json
from zavod import Context
from zavod import helpers as h


def map_gender(gender_code):
    gender_map = {
        "V": "male",
        "M": "female",  # Assuming "M" stands for female in Lithuanian
    }
    return gender_map.get(gender_code)


# Error handling function
def check_data_limits(data):
    num_found = data.get("numFound")
    if num_found > 500:
        raise ValueError(
            f"Error: numFound ({num_found}) is greater than the pre-set limit of 500."
        )


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(
        path, mime_type="application/json", title=context.SOURCE_TITLE
    )

    # Open the file and process its content within the `with` block
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    # Checking data limits
    check_data_limits(data)

    for entry in data.get("list", []):
        # Create a Person entity
        person = context.make("Person")
        person_id = context.make_id(entry.get("vardas"), entry.get("pavarde"))
        person.id = person_id
        person.add("name", f"{entry.get('vardas')} {entry.get('pavarde')}", lang="lit")
        h.apply_date(person, "birthDate", entry.get("gimimoData"))
        person.add("topics", "sanction")
        gender = map_gender(entry.get("lytis"))
        if gender:
            person.add("gender", gender)

        # If citizenships exist, add them
        citizenships = entry.get("pilietybes", [])
        for citizenship in citizenships:
            code = (
                citizenship.get("valstybe", {}).get("pilietybeClaEntry", {}).get("key")
            )
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
