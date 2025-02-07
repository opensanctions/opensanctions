import sys
import yaml
import json
from pathlib import Path

BASE_PATH = "data/datasets"


def extract_schemata(dataset_name: str):
    json_path = Path(BASE_PATH) / dataset_name / "statistics.json"

    if not json_path.exists():
        print(f"Error: The file {json_path} does not exist.")
        sys.exit(1)

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Access the correct "schemata" which contains dictionaries
    schemata = data.get("targets", {}).get("schemata", [])

    schema_entities = {}
    for schema in schemata:
        if isinstance(schema, dict):
            name = schema.get("name")
            count = schema.get("count")
            if name and count is not None:
                schema_entities[name] = count
        else:
            print("Warning: Found a schema entry that is not a dictionary. Skipping.")

    # Compute the min and max values for the entities
    min_values = {name: int(count * 0.85) for name, count in schema_entities.items()}
    max_values = {name: count * 2 for name, count in schema_entities.items()}

    # Return the assertions in the required YAML format
    return {
        "assertions": {
            "min": {"schema_entities": min_values},
            "max": {"schema_entities": max_values},
        }
    }


def main():
    if len(sys.argv) != 2:
        print("Usage: python zavod/zavod/tools/assert.py <dataset_name>")
        sys.exit(1)

    dataset_name = sys.argv[1]

    # Extract schemata and create YAML assertions
    assertions = extract_schemata(dataset_name)

    # Output the YAML formatted assertions
    print(yaml.dump(assertions, default_flow_style=False, sort_keys=False))


if __name__ == "__main__":
    main()
