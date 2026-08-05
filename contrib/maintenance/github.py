"""GitHub-side state: the outage board and existing autofix pull requests."""

import argparse
import os
import re
import sys
from enum import StrEnum
from typing import Any

from . import session

# Outages are tracked on the org-level GitHub Projects v2 board #6, not as plain
# issues. The board carries two custom fields we care about: `dataset` (which
# dataset the item is about) and `Status` (whether it's a passing "Issue" or an
# active "Outage"). Field ids are stable and mirror site/lib/github.ts.
GITHUB_API = "https://api.github.com"
GITHUB_GRAPHQL = "https://api.github.com/graphql"
GITHUB_REPO = os.environ.get("GITHUB_REPOSITORY", "opensanctions/opensanctions")
PROJECT_ORG = "opensanctions"
PROJECT_NUMBER = 6
FIELD_DATASET = 271254866
FIELD_STATUS = 271254605


class BoardStatus(StrEnum):
    """The `Status` values the board distinguishes.

    `ISSUE` is breakage that is ours to fix — a source redesign, newly added bot
    protection — and unlikely to resolve itself. `OUTAGE` is a source that is
    merely down and expected back, which suppresses the issues agent (see
    `get_outage_datasets`). `DONE` is resolved.
    """

    ISSUE = "Issue"
    OUTAGE = "Outage"
    DONE = "Done"


OUTAGE_STATUS = BoardStatus.OUTAGE

# Writing to the board goes through GraphQL, which addresses the project and its
# fields by node id — a different id space from the numeric REST ids above, so the
# two are not interchangeable. Single-select values are set by option id, not by
# name, hence the mapping.
PROJECT_NODE_ID = "PVT_kwDOBRLMhs4BTbYq"
FIELD_DATASET_NODE_ID = "PVTF_lADOBRLMhs4BTbYqzhArBVI"
FIELD_STATUS_NODE_ID = "PVTSSF_lADOBRLMhs4BTbYqzhArBE0"
STATUS_OPTION_IDS = {
    BoardStatus.ISSUE: "f75ad846",
    BoardStatus.OUTAGE: "47fc9ee4",
    BoardStatus.DONE: "98236657",
}


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


def search_dataset_issues(name: str, state: str = "all") -> list[dict[str, Any]]:
    """Return issues mentioning a dataset, newest first, for finding prior art.

    Recurring breakage is the norm, so a new report should reference what is
    already on record. Returns the raw search items (`number`, `title`, `state`,
    `html_url`, `created_at`, ...) rather than a narrowed shape, since what is
    worth quoting differs case by case.
    """
    query = f"repo:{GITHUB_REPO} is:issue {name}"
    if state != "all":
        query = f"{query} is:{state}"
    response = session.get(
        f"{GITHUB_API}/search/issues",
        params={"q": query, "sort": "created", "order": "desc", "per_page": "20"},
        headers=_github_headers(),
    )
    response.raise_for_status()
    items = response.json().get("items", [])
    assert isinstance(items, list), f"Unexpected search response: {items!r}"
    return items


def _graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    """Run a GraphQL request against the GitHub API and return its `data`.

    Board writes always need a token — unlike the read path, which relies on the
    project being public. GraphQL reports application-level failures as an
    `errors` array alongside HTTP 200, so those are checked explicitly.
    """
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN is required to write to the project board")
    response = session.post(
        GITHUB_GRAPHQL,
        json={"query": query, "variables": variables},
        headers={"Authorization": f"Bearer {token}"},
    )
    response.raise_for_status()
    payload = response.json()
    errors = payload.get("errors")
    if errors:
        messages = "; ".join(error.get("message", str(error)) for error in errors)
        raise RuntimeError(f"GraphQL error: {messages}")
    data = payload.get("data")
    assert isinstance(data, dict), f"Unexpected GraphQL response: {payload!r}"
    return data


def get_issue_node_id(number: int) -> str:
    """Return the GraphQL node id of an issue, which the board mutations take."""
    data = _graphql(
        """
        query ($owner: String!, $repo: String!, $number: Int!) {
          repository(owner: $owner, name: $repo) {
            issue(number: $number) { id }
          }
        }
        """,
        {
            "owner": GITHUB_REPO.split("/")[0],
            "repo": GITHUB_REPO.split("/")[1],
            "number": number,
        },
    )
    issue = (data.get("repository") or {}).get("issue")
    if issue is None:
        raise RuntimeError(f"No such issue: {GITHUB_REPO}#{number}")
    node_id = issue["id"]
    assert isinstance(node_id, str), f"Unexpected issue id: {node_id!r}"
    return node_id


def add_issue_to_board(issue_node_id: str) -> str:
    """Put an issue on the board and return its project item id.

    Idempotent: an issue already on the board yields its existing item id, so
    this is safe to call when re-marking an item whose status went stale.
    """
    data = _graphql(
        """
        mutation ($project: ID!, $content: ID!) {
          addProjectV2ItemById(input: {projectId: $project, contentId: $content}) {
            item { id }
          }
        }
        """,
        {"project": PROJECT_NODE_ID, "content": issue_node_id},
    )
    item_id = data["addProjectV2ItemById"]["item"]["id"]
    assert isinstance(item_id, str), f"Unexpected item id: {item_id!r}"
    return item_id


def _set_board_field(item_id: str, field_id: str, value: dict[str, Any]) -> None:
    """Set one custom field on a board item."""
    _graphql(
        """
        mutation ($project: ID!, $item: ID!, $field: ID!,
                  $value: ProjectV2FieldValue!) {
          updateProjectV2ItemFieldValue(input: {
            projectId: $project, itemId: $item, fieldId: $field, value: $value
          }) { projectV2Item { id } }
        }
        """,
        {
            "project": PROJECT_NODE_ID,
            "item": item_id,
            "field": field_id,
            "value": value,
        },
    )


def mark_issue_on_board(number: int, dataset: str, status: BoardStatus) -> str:
    """Add an issue to the board with its dataset and status set, returning the item id.

    Both fields matter for `OUTAGE`: `get_outage_datasets` keys on `dataset`, so
    leaving it unset means the status is recorded but the issues agent is never
    actually suppressed. That filter also only considers open issues, so marking
    a closed issue has no effect until it is reopened.
    """
    item_id = add_issue_to_board(get_issue_node_id(number))
    _set_board_field(item_id, FIELD_DATASET_NODE_ID, {"text": dataset})
    _set_board_field(
        item_id,
        FIELD_STATUS_NODE_ID,
        {"singleSelectOptionId": STATUS_OPTION_IDS[status]},
    )
    return item_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect and update the Crawler Issues Page board."
    )
    commands = parser.add_subparsers(dest="command", required=True)

    mark = commands.add_parser("mark", help="set an issue's board status")
    mark.add_argument("number", type=int, help="issue number to mark")
    mark.add_argument("dataset", help="dataset name the issue is about")
    mark.add_argument(
        "status",
        type=BoardStatus,
        choices=list(BoardStatus),
        help="Outage for a source expected back, Issue for breakage that is ours",
    )

    commands.add_parser(
        "outages", help="list datasets with an active outage, which the agent skips"
    )

    search = commands.add_parser(
        "search", help="find past issues for a dataset, newest first"
    )
    search.add_argument("dataset", help="dataset name to search for")

    args = parser.parse_args()
    try:
        if args.command == "mark":
            item_id = mark_issue_on_board(args.number, args.dataset, args.status)
            print(
                f"{GITHUB_REPO}#{args.number}: {args.status} ({args.dataset}) -> {item_id}"
            )
        elif args.command == "search":
            for issue in search_dataset_issues(args.dataset):
                print(f"{issue['number']}\t{issue['state']}\t{issue['title']}")
        else:
            for dataset in sorted(get_outage_datasets()):
                print(dataset)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
