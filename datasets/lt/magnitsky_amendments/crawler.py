import json
from zavod import Context
from zavod import helpers as h

DATE_FORMATS = ["%Y-%m-%d"]


def map_gender(gender_code):
    gender_map = {
        "V": "male",
        "M": "female",  # Assuming "M" stands for female in Lithuanian
    }
    return gender_map.get(gender_code)


# Error handling function
def check_data_limits(data):
    num_found = data.get("numFound")
    if num_found > 400:
        raise ValueError(
            f"Error: numFound ({num_found}) is greater than the pre-set limit of 400."
        )


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(
        path, mime_type="application/json", title=context.SOURCE_TITLE
    )

    # Open the file and process its content within the `with` block
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
        print(f"Fetched data: {data}")  # Debug statement to confirm data fetch

    # Checking data limits
    check_data_limits(data)

    for entry in data.get("list", []):
        print(f"Processing entry: {entry}")  # Debug statement for each entry

        # Create a Person entity
        person = context.make("Person")
        person_id = context.make_id(entry.get("vardas"), entry.get("pavarde"))
        person.id = person_id
        person.add("name", f"{entry.get('vardas')} {entry.get('pavarde')}", lang="lit")
        person.add("birthDate", h.parse_date(entry.get("gimimoData"), DATE_FORMATS))
        person.add("topics", "sanction")
        gender = map_gender(entry.get("lytis"))
        if gender:
            person.add("gender", gender)

        # If nationalities exist, add them
        nationalities = entry.get("pilietybes", [])
        for nationality in nationalities:
            code = (
                nationality.get("valstybe", {}).get("pilietybeClaEntry", {}).get("key")
            )
            if code:
                person.add("nationality", code, lang="lit")

        # Create a Sanction entity
        sanction = h.make_sanction(context, person)
        sanction.add("reason", entry.get("priezastis"), lang="lit")
        # sanction.add("startDate", h.parse_date("2018-01-01", DATE_FORMATS))
        sanction.add("endDate", h.parse_date(entry.get("uzdraustaIki"), DATE_FORMATS))

        # Emit entities
        context.emit(person, target=True)
        context.emit(sanction)
