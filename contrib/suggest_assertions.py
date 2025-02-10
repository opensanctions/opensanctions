import sys
import yaml
import requests


def fetch_json(url: str):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def extract_schemata(dataset_name: str):
    # Base URL for retrieving the dataset information
    index_url = (
        f"https://data.opensanctions.org/datasets/latest/{dataset_name}/index.json"
    )
    index_json = fetch_json(index_url)

    # Extract the statistics URL
    statistics_url = index_json.get("statistics_url")
    if not statistics_url:
        raise ValueError("Statistics URL not found in the dataset index.")

    # Fetch statistics JSON from the provided URL
    data = fetch_json(statistics_url)

    # Extract schema entity counts
    schema_entities = {
        schema["name"]: schema["count"] for schema in data["things"]["schemata"]
    }
    # Compute min (85%) and max (200%) thresholds
    min_values = {name: int(count * 0.85) for name, count in schema_entities.items()}
    max_values = {name: count * 2 for name, count in schema_entities.items()}

    return {
        "assertions": {
            "min": {"schema_entities": min_values},
            "max": {"schema_entities": max_values},
        }
    }


def main():
    dataset_name = sys.argv[1] if len(sys.argv) == 2 else None
    if not dataset_name:
        raise SystemExit("Usage: python contrib/suggest_assertions.py <dataset_name>")
    assertions = extract_schemata(dataset_name)
    # Output YAML assertions
    print(yaml.dump(assertions, default_flow_style=False, sort_keys=False))


if __name__ == "__main__":
    main()
