from datasets.be.fod_sanctions.crawler import normalize_birth_date


def test_normalize_birth_date() -> None:
    # Two-digit years pivoting into the future are pulled back a century.
    assert normalize_birth_date("16-07-68") == "1968-07-16"
    assert normalize_birth_date("23-10-64") == "1964-10-23"
    # Years already in the past are untouched.
    assert normalize_birth_date("01-01-95") == "1995-01-01"
    assert normalize_birth_date("01-01-05") == "2005-01-01"
    # Non-conforming values pass through untouched.
    assert normalize_birth_date("N/A") == "N/A"
    assert normalize_birth_date("") == ""
