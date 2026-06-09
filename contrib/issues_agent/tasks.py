import os
import re
import sys
import json
import yaml
import hashlib
from typing import Any, List, Optional, Tuple
import requests
from pathlib import Path
from jinja2 import Template


session = requests.Session()
session.headers.update({"User-Agent": "os-issues-agent/1.0"})
repo_path_ = os.environ.get("GITHUB_WORKSPACE", ".")
datasets_path = Path(repo_path_) / "datasets"

INDEX_URL = "https://data.opensanctions.org/datasets/latest/index.json"
MAX_ISSUES = 1000
# Rendered per dataset with the diagnostic context (paths, branch, ci_test) so
# the prompt can branch between lookup-only and code-fix instructions. autoescape
# stays off (default) so markdown and YAML examples pass through untouched.
PROMPT = Template(
    (Path(__file__).parent / "prompt.md").read_text(),
    trim_blocks=True,
    lstrip_blocks=True,
)

# Outages are tracked on the org-level GitHub Projects v2 board #6, not as plain
# issues. The board carries two custom fields we care about: `dataset` (which
# dataset the item is about) and `Status` (whether it's a passing "Issue" or an
# active "Outage"). Field ids are stable and mirror site/lib/github.ts.
GITHUB_API = "https://api.github.com"
GITHUB_REPO = os.environ.get("GITHUB_REPOSITORY", "opensanctions/opensanctions")
PROJECT_ORG = "opensanctions"
PROJECT_NUMBER = 6
FIELD_DATASET = 271254866
FIELD_STATUS = 271254605
OUTAGE_STATUS = "Outage"


def get_path_from_name(name: str) -> str:
    for path in datasets_path.glob("**/*.y*ml"):
        if path.stem == name:
            return path.as_posix()
    raise RuntimeError(f"Dataset {name!r} not found in: {datasets_path}")


# Python object reprs carry a memory address (e.g. "<Element div at 0x7f...>")
# that changes every run. The address tells us nothing about which issue this is,
# so strip it before checksumming.
HEX_ADDR_RE = re.compile(r"0x[0-9a-fA-F]+")


def issues_checksum(issues: List[Any]) -> str:
    """Return a checksum that identifies a constant set of crawl issues.

    Use this to seed a deterministic branch name: two crawls that surface the
    same warnings hash to the same value, so a re-run over an unchanged issue
    set reuses the branch (and dedupes against an existing PR), while a genuinely
    changed set produces a new branch.

    Only run-invariant content is hashed. Each issue's `timestamp` and `id` are
    dropped — `id` is itself a hash that folds in the timestamp, so it churns
    every run — and memory addresses are scrubbed from the remaining fields. The
    page-content hashes that live in `data` (e.g. the expected/actual values of a
    "DOM hash changed" warning) are stable and are kept, so a real source change
    still moves the checksum.
    """
    normalized = [
        {k: issue.get(k) for k in ("level", "module", "message", "entity", "data")}
        for issue in issues
    ]
    # Sort so the order issues happen to appear in the log doesn't affect the hash.
    normalized.sort(key=lambda i: json.dumps(i, sort_keys=True, ensure_ascii=False))
    canonical = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
    canonical = HEX_ADDR_RE.sub("0x*", canonical)
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()


def get_outage_datasets() -> set[str]:
    """Return the names of datasets that have an active outage on the project board.

    Use this to skip datasets whose source is known to be down: their warnings
    are a symptom of the outage, not something a lookup or code change can fix,
    so the agent should leave them for the humans tracking the outage.

    Reads the public Projects v2 board #6 (no auth needed — the project is
    public), paginating via the Link header, and keeps items whose `Status`
    field is "Outage". Mirrors getOpenIssues() in site/lib/github.ts.
    """
    headers = {
        "Accept": "application/vnd.github+json",
        # Required header for the Projects v2 REST API (currently in preview).
        "X-GitHub-Api-Version": "2026-03-10",
    }
    url: str | None = (
        f"{GITHUB_API}/orgs/{PROJECT_ORG}/projectsV2/{PROJECT_NUMBER}/items"
        f"?fields[]={FIELD_DATASET}&fields[]={FIELD_STATUS}&q=is:open&per_page=100"
    )
    outages: set[str] = set()
    while url is not None:
        response = session.get(url, headers=headers)
        response.raise_for_status()

        # Each field value has a different shape depending on its data_type.
        for item in response.json():
            if item.get("content_type") != "Issue":
                continue
            dataset: str | None = None
            status: str | None = None
            for field in item.get("fields", []):
                if field["id"] == FIELD_DATASET:
                    dataset = field.get("value", {}).get("raw")
                elif field["id"] == FIELD_STATUS:
                    status = field.get("value", {}).get("name", {}).get("raw")
            if dataset is not None and status == OUTAGE_STATUS:
                outages.add(dataset)

        # Follow the `Link: <url>; rel="next"` header until exhausted.
        next_link = response.links.get("next")
        url = next_link["url"] if next_link is not None else None

    return outages


def pr_exists(branch: str) -> bool:
    """Return True if a PR for this branch already exists, in any state.

    The branch name encodes a checksum of the issue set, so a PR whose head is
    this branch — whether open, merged, or closed-unmerged — means this exact
    set of warnings has already been handled: open (awaiting review), merged
    (fixed, but the published index still shows the old issues until the next
    crawl), or closed (a human rejected this fix). In every case we skip rather
    than re-open, which is what makes the agent idempotent across daily runs.

    Searches as the authenticated `GITHUB_TOKEN` when present (required for the
    search API in CI); falls back to unauthenticated for local runs.
    """
    headers = {"Accept": "application/vnd.github+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = session.get(
        f"{GITHUB_API}/search/issues",
        params={"q": f"repo:{GITHUB_REPO} is:pr head:{branch}"},
        headers=headers,
    )
    response.raise_for_status()
    return response.json().get("total_count", 0) > 0


def get_code_path(yaml_path: str, entry_point: Optional[str]) -> Optional[str]:
    """Resolve a dataset's entry_point to the crawler source file, if it has one.

    Use this to give the agent the actual code to read and fix, not just the
    metadata YAML. Mirrors zavod's loader (zavod/runtime/loader.py): an
    entry_point naming an installed module — e.g. `zavod.runner.enrich:enrich`,
    used by enrichment datasets — has no dataset-local code to edit, so return
    None. Otherwise the entry_point names a file relative to the dataset
    directory (`crawler.py`, `crawler`, `ofac_advanced.py:crawl`); return its path.
    """
    if entry_point is None:
        return None
    module_name = entry_point.split(":", 1)[0]
    base = Path(yaml_path).parent
    for candidate in (module_name, f"{module_name}.py"):
        file_path = base / candidate
        if file_path.is_file():
            return file_path.as_posix()
    return None


def read_dataset_meta(yaml_path: str) -> Tuple[Optional[str], bool]:
    """Return the `entry_point` and `ci_test` flag from a dataset YAML.

    `ci_test` indicates whether the crawler can run in CI at all: it is set to
    false for crawlers that need credentials we don't have in CI (Zyte, GPT
    keys) or are too slow. It tells the agent whether re-running the crawler to
    verify a fix is even possible. Defaults to True, matching the zavod model.
    """
    with open(yaml_path, "r") as fh:
        data = yaml.safe_load(fh)
    return data.get("entry_point"), data.get("ci_test", True)


def get_issue_details(issue_url):
    try:
        response = session.get(issue_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching issue details: {e}")
        return None


def log(message: str) -> None:
    """Emit a diagnostic line to stderr.

    stdout carries the matrix JSON that GitHub Actions captures, so anything we
    want a human to read in the run log has to go to stderr to avoid corrupting it.
    """
    print(message, file=sys.stderr)


def index_jobs():
    response = session.get(INDEX_URL)
    response.raise_for_status()
    index_data = response.json()
    outage_datasets = get_outage_datasets()
    tasks: List[Any] = []

    for dataset in index_data.get("datasets", []):
        name = dataset.get("name")
        if not name:
            continue
        levels = dataset.get("issue_levels", {})
        warnings = levels.get("warning", 0)
        errors = levels.get("error", 0)

        if warnings == 0:
            continue
        if name in outage_datasets:
            log(f"Documented outage: {name}")
            continue
        if (warnings + errors) > MAX_ISSUES:
            log(f"Fubar: {name}")
            continue

        issues = get_issue_details(dataset.get("issues_url")) or {}
        checksum = issues_checksum(issues.get("issues", []))
        branch = f"autofix/{name.replace('_', '-')}-{checksum[:10]}"
        if pr_exists(branch):
            log(f"PR exists: {name} ({branch})")
            continue

        path = get_path_from_name(name)
        entry_point, ci_test = read_dataset_meta(path)
        code_path = get_code_path(path, entry_point)

        prompt = PROMPT.render(
            name=name,
            issues_url=dataset.get("issues_url"),
            yaml_path=path,
            branch=branch,
            code_path=code_path,
            ci_test=ci_test,
        )
        title = f"[{name}]: {warnings} warnings"
        tasks.append({"prompt": prompt, "name": title, "branch": branch})

    print(json.dumps(tasks))


if __name__ == "__main__":
    index_jobs()
