import pytest

from zavod.runtime.safety import XSS_SUSPECT_PATTERN


@pytest.mark.parametrize(
    "value",
    [
        "<script>alert(1)</script>",
        "click <a href='x'>here</a>",
        'href="javascript:alert(1)"',
        "src=data:text/html;base64,AAAA",
        " data:text/html",
        "onload=alert(1)",
        "&#x41;",
    ],
)
def test_xss_pattern_matches(value: str) -> None:
    assert XSS_SUSPECT_PATTERN.search(value) is not None


@pytest.mark.parametrize(
    "value",
    [
        # "data:", "javascript:" etc. embedded in a longer word must not match: the
        # scheme alternatives require a word boundary before them.
        "POSLANIČKOG MANDATA: 2. Decembar 2020.",
        "ERRATA: see footnote",
        "plain biography text without markup",
    ],
)
def test_xss_pattern_ignores_words_ending_in_scheme(value: str) -> None:
    assert XSS_SUSPECT_PATTERN.search(value) is None
