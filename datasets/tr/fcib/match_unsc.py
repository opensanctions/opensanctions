import requests
import os
import csv


OS_API_KEY = os.getenv("ZAVOD_OPENSANCTIONS_API_KEY")
if not OS_API_KEY:
    raise ValueError("The ZAVOD_OPENSANCTIONS_API_KEY environment variable is not set")

headers = {
    "Authorization": OS_API_KEY,
}

with open("output.csv") as fh:
    reader = csv.DictReader(fh)

    for idx, row in enumerate(reader, 1):
        print(idx, row["full_name"], row["date_of_birth_iso"], row["place_of_birth"], row["nationality"])
        entity = {
            "schema": "LegalEntity",
            "properties": {
                "name": [row["full_name"], row["original_script_name"]],
                "birthDate": row["date_of_birth_iso"].split("\n"),
                "birthPlace": row["place_of_birth"],
                "alias": row["aliases"].split("\n"),
            }
        }
        query = {
            "queries": {
                "q1": entity
            }
        }

        response = requests.post(
            "https://api.opensanctions.org/match/un_sc_sanctions", headers=headers, json=query
        )
        response.raise_for_status()

        results = response.json()["responses"]["q1"]["results"]

        matches = [result for result in results if result["match"]]
        if matches:
            continue

        print("    ", "no match")
        for result in results:
            print(result["score"], result["match"], f'https://www.opensanctions.org/entities/{result["id"]}')
