import os
import re
import sys
import json
import yaml
import hashlib
from typing import Any, Dict, List, Optional
import requests
from pathlib import Path
from jinja2 import Template


session = requests.Session()
session.headers.update({"User-Agent": "os-issues-agent/1.0"})
repo_path_ = os.environ.get("GITHUB_WORKSPACE", ".")
datasets_path = Path(repo_path_) / "datasets"

INDEX_URL = "https://data.opensanctions.org/datasets/latest/index.json"
MAX_ISSUES = 1000

# Match compute to task difficulty. Lookup/assertion edits on YAML are mechanical,
# so a mid-tier model with few turns suffices. Datasets with a crawler may need an
# actual code fix — stronger reasoning, plus turns to run mypy, crawl, and iterate.
MODEL_LOOKUP = "claude-sonnet-4-6"
MODEL_CODE = "claude-opus-4-8"
MAX_TURNS_LOOKUP = 30
MAX_TURNS_CODE_VERIFIABLE = 100  # ci_test true: edit, then crawl-verify and iterate
MAX_TURNS_CODE_BLIND = 60  # ci_test false: edit + mypy/ruff, no crawl loop
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
                # `value` is present-but-null when a field exists on the board
                # but is unset for this item (e.g. a fresh outage report with no
                # Dataset assigned), so `.get("value", {})` won't shield us — the
                # key is there, just null. Coerce each level with `or {}`.
                if field["id"] == FIELD_DATASET:
                    dataset = (field.get("value") or {}).get("raw")
                elif field["id"] == FIELD_STATUS:
                    name = (field.get("value") or {}).get("name") or {}
                    status = name.get("raw")
            if dataset is not None and status == OUTAGE_STATUS:
                outages.add(dataset)

        # Follow the `Link: <url>; rel="next"` header until exhausted.
        next_link = response.links.get("next")
        url = next_link["url"] if next_link is not None else None

    return outages


def _github_headers() -> Dict[str, str]:
    """Headers for the GitHub REST API, authenticated when a token is present.

    A token is required for the search API rate limit in CI; local runs fall
    back to unauthenticated access (fine for this public repo).
    """
    headers = {"Accept": "application/vnd.github+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def branch_prefix(name: str) -> str:
    """The branch-name prefix shared by every autofix PR for a dataset."""
    return f"autofix/{name.replace('_', '-')}-"


def get_open_autofix_branches() -> set[str]:
    """Return every open PR head branch under the `autofix/` prefix.

    Fetched once per run (open PRs are few) and matched locally, rather than
    listing PRs once per dataset.
    """
    branches: set[str] = set()
    url: Optional[str] = (
        f"{GITHUB_API}/repos/{GITHUB_REPO}/pulls?state=open&per_page=100"
    )
    while url is not None:
        response = session.get(url, headers=_github_headers())
        response.raise_for_status()
        for pr in response.json():
            ref = pr["head"]["ref"]
            if ref.startswith("autofix/"):
                branches.add(ref)
        next_link = response.links.get("next")
        url = next_link["url"] if next_link is not None else None
    return branches


def dataset_has_open_pr(name: str, open_branches: set[str]) -> bool:
    """Return True if an open autofix PR already targets this dataset.

    Matched by branch prefix, not by the full checksum: for datasets with
    drifting counts the checksum changes every run, so an exact-branch check
    would never catch yesterday's still-open PR and we'd open a fresh one daily.
    One open proposal per dataset at a time is enough.
    """
    pattern = re.compile(rf"^{re.escape(branch_prefix(name))}[0-9a-f]+$")
    return any(pattern.match(branch) for branch in open_branches)


def has_closed_pr_for_branch(branch: str) -> bool:
    """Return True if a CLOSED or merged PR already used this exact branch.

    The branch encodes the issue-set checksum, so a closed/merged match means
    this precise set of warnings was already handled — merged (fixed; the
    published index still shows it until the next crawl) or closed (a human
    rejected it). Don't re-propose the identical set. `is:closed` includes
    merged PRs.
    """
    response = session.get(
        f"{GITHUB_API}/search/issues",
        params={"q": f"repo:{GITHUB_REPO} is:pr is:closed head:{branch}"},
        headers=_github_headers(),
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


def read_dataset_meta(yaml_path: str) -> Dict[str, Any]:
    """Load local operational metadata that is absent from the public catalog."""
    with open(yaml_path, "r") as fh:
        return yaml.safe_load(fh)


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
    open_autofix_branches = get_open_autofix_branches()
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

        if dataset_has_open_pr(name, open_autofix_branches):
            log(f"Open PR exists: {name}")
            continue

        issues = get_issue_details(dataset.get("issues_url")) or {}
        checksum = issues_checksum(issues.get("issues", []))
        branch = f"{branch_prefix(name)}{checksum[:10]}"
        if has_closed_pr_for_branch(branch):
            log(f"Already handled (closed PR): {name} ({branch})")
            continue

        path = get_path_from_name(name)
        crawler_dir = Path(path).parent.as_posix()
        dataset_meta = read_dataset_meta(path)
        entry_point = dataset_meta.get("entry_point")
        ci_test = dataset_meta.get("ci_test", True)
        code_path = get_code_path(path, entry_point)

        # Only crawler datasets can require code, so only they need zavod
        # installed (for mypy and crawling) and the heavier model/turn budget.
        if code_path is not None:
            model = MODEL_CODE
            max_turns = MAX_TURNS_CODE_VERIFIABLE if ci_test else MAX_TURNS_CODE_BLIND
            needs_zavod = True
        else:
            model = MODEL_LOOKUP
            max_turns = MAX_TURNS_LOOKUP
            needs_zavod = False

        deploy_config = dataset_meta.get("deploy", {})
        max_turns = deploy_config.get("issues_turns", max_turns)
        if max_turns > MAX_TURNS_CODE_VERIFIABLE:
            model = MODEL_CODE

        prompt = PROMPT.render(
            name=name,
            issues_url=dataset.get("issues_url"),
            yaml_path=path,
            crawler_dir=crawler_dir,
            branch=branch,
            code_path=code_path,
            ci_test=ci_test,
        )
        title = f"[{name}]: {warnings} warnings"
        tasks.append(
            {
                "prompt": prompt,
                "name": title,
                "branch": branch,
                "model": model,
                "max_turns": max_turns,
                "needs_zavod": needs_zavod,
            }
        )

    print(json.dumps(tasks))


if __name__ == "__main__":
    index_jobs()
