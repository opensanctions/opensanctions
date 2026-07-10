"""GitHub-side state: the outage board and existing autofix pull requests."""

import os
import re

from . import session

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


def _github_headers() -> dict[str, str]:
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
    url: str | None = f"{GITHUB_API}/repos/{GITHUB_REPO}/pulls?state=open&per_page=100"
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
    count = response.json().get("total_count", 0)
    assert isinstance(count, int), f"Unexpected search response: {count!r}"
    return count > 0
