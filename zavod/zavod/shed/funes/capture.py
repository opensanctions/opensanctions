"""Inspectability check for captured Pravda snapshots."""

from pravda import Snapshot


def first_error_line(error: str | None) -> str | None:
    if error is None:
        return None
    return next(
        (line.strip() for line in error.splitlines() if line.strip()),
        None,
    )


def inspectability_issue(snapshot: Snapshot) -> str | None:
    if snapshot.rendered_html:
        return None
    if snapshot.error:
        # Playwright errors are multiline logs; keep only the first line.
        diagnostics = first_error_line(snapshot.error)
    elif snapshot.http_status is not None and snapshot.http_status >= 400:
        diagnostics = f"http status {snapshot.http_status}"
    else:
        diagnostics = None
    reason = "missing artifact: rendered_html"
    if diagnostics:
        return f"{reason} ({diagnostics})"
    return reason
