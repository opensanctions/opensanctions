import sys
import requests
from collections import Counter


def aggregate_issues(dataset: str) -> None:
    url = f"https://data.opensanctions.org/datasets/latest/{dataset}/index.json"
    index_response = requests.get(url)
    index_data = index_response.json()
    issues_url = index_data["issues_url"]

    issues_response = requests.get(issues_url)
    issues_data = issues_response.json()
    issues = issues_data.get("issues", [])
    counter = Counter()
    for issue in issues:
        message = issue.get("message", "").strip()
        if not len(message):
            continue
        counter[message] += 1

    # Process the issues_data as needed
    for message, count in counter.most_common(100):
        print(f"[{count}]: {message}")


if __name__ == "__main__":
    dataset = sys.argv[1]
    aggregate_issues(dataset)
