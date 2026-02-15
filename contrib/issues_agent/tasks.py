import os
import json
from typing import Any, List
import requests
from pathlib import Path


session = requests.Session()
session.headers.update({"User-Agent": "os-issues-agent/1.0"})
repo_path_ = os.environ.get("GITHUB_WORKSPACE", ".")
datasets_path = Path(repo_path_) / "datasets"

INDEX_URL = "https://data.opensanctions.org/datasets/latest/index.json"
PROMPT = open(Path(__file__).parent / "prompt.md", "r").read()


def get_path_from_name(name: str) -> str:
    for path in datasets_path.glob("**/*.y*ml"):
        if path.stem == name:
            return path.as_posix()
    raise RuntimeError(f"Dataset {name!r} not found in: {datasets_path}")


def get_issue_details(issue_url):
    try:
        response = session.get(issue_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching issue details: {e}")
        return None


def index_jobs():
    response = session.get(INDEX_URL)
    response.raise_for_status()
    index_data = response.json()
    tasks: List[Any] = []

    for dataset in index_data.get("datasets", []):
        levels = dataset.get("issue_levels", [])
        warnings = levels.get("warning", 0)
        if warnings == 0:
            continue
        name = dataset.get("name")
        if not name:
            # print("Dataset entry missing 'name' field.")
            continue

        path = get_path_from_name(name)
        # print(f"Dataset: {name}, Issues: {levels}, Path: {path}")

        prompt = str(PROMPT)
        prompt = prompt.replace("{NAME}", name)
        prompt = prompt.replace("{ISSUES_URL}", dataset.get("issues_url"))
        prompt = prompt.replace("{PATH}", path)
        tasks.append({"prompt": prompt})

    print(json.dumps(tasks))


if __name__ == "__main__":
    index_jobs()
