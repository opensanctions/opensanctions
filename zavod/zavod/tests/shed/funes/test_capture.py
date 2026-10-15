"""Tests for the snapshot inspectability boundary."""

import uuid
from datetime import UTC, datetime
from typing import Any

from pravda import Snapshot

from zavod.shed.funes.capture import first_error_line, inspectability_issue

NOW = datetime(2025, 1, 1, tzinfo=UTC)


def test_first_error_line() -> None:
    assert first_error_line(None) is None
    assert first_error_line("\n \n") is None
    assert first_error_line("TimeoutError: x") == "TimeoutError: x"
    # Playwright errors prefix a blank line before the first log line.
    assert first_error_line("\nTimeoutError: x\nsecond line") == "TimeoutError: x"


def make_snapshot(**overrides: Any) -> Snapshot:
    """One inspectable-by-default snapshot."""
    fields: dict[str, Any] = {
        "id": uuid.uuid4(),
        "url": "https://example.org/",
        "final_url": "https://example.org/",
        "captured_at": NOW,
        "http_status": 200,
        "error": None,
        "rendered_html": "storage/example.org/page.html",
        "screenshot": None,
        "http_archive": None,
    }
    fields.update(overrides)
    return Snapshot(**fields)


def test_complete_200() -> None:
    assert inspectability_issue(make_snapshot()) is None


def test_complete_403() -> None:
    assert inspectability_issue(make_snapshot(http_status=403)) is None


def test_complete_408() -> None:
    assert inspectability_issue(make_snapshot(http_status=408)) is None


def test_capture_error_with_artifacts() -> None:
    assert inspectability_issue(make_snapshot(error="timeout after capture")) is None


def test_navigation_error_no_artifacts() -> None:
    issue = inspectability_issue(
        make_snapshot(
            error="net::ERR_NAME_NOT_RESOLVED",
            http_status=None,
            rendered_html=None,
        )
    )
    assert issue is not None
    assert "rendered_html" in issue
    assert "ERR_NAME_NOT_RESOLVED" in issue


def test_missing_rendered_html_only() -> None:
    issue = inspectability_issue(make_snapshot(rendered_html=None))
    assert issue == "missing artifact: rendered_html"


def test_missing_rendered_html_no_diagnostics() -> None:
    issue = inspectability_issue(make_snapshot(rendered_html=None, http_status=None))
    assert issue == "missing artifact: rendered_html"


def test_missing_rendered_html_http_status_diagnostic() -> None:
    issue = inspectability_issue(make_snapshot(rendered_html=None, http_status=404))
    assert issue == "missing artifact: rendered_html (http status 404)"


def test_optional_screenshot_and_har() -> None:
    snapshot = make_snapshot(
        screenshot="storage/example.org/shot.png",
        http_archive={"log": {"entries": []}},
    )
    assert inspectability_issue(snapshot) is None


def test_multiline_error_truncated_to_first_line() -> None:
    error = (
        "TimeoutError: Timeout 30000ms exceeded.\n"
        "=========================== logs ===========================\n"
        'navigating to "https://example.org/", waiting until "load"\n'
        "============================================================"
    )
    issue = inspectability_issue(make_snapshot(error=error, rendered_html=None))
    assert (
        issue
        == "missing artifact: rendered_html (TimeoutError: Timeout 30000ms exceeded.)"
    )
