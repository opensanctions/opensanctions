from zavod.helpers.dates import parse_date, check_no_year, extract_years, backdate
from datetime import datetime

FORMATS = ["%b %Y", "%d.%m.%Y", "%Y-%m"]


def test_extract_years():
    assert len(extract_years("foo")) == 0
    assert len(extract_years("25.2.")) == 0
    assert len(extract_years("1602")) == 0
    assert len(extract_years("3572")) == 0
    assert len(extract_years("1903")) == 1
    assert len(extract_years("2023")) == 1
    assert len(extract_years("circa 2023")) == 1
    assert len(extract_years("between 1980 and 1982")) == 2


def test_check_no_year():
    assert check_no_year(None) is True
    assert check_no_year("foo") is True
    assert check_no_year("25.2.") is True
    assert check_no_year("25.") is True
    assert check_no_year("with 2011") is False


def test_parse_date():
    assert parse_date("foo", FORMATS) == ["foo"]
    assert parse_date(None, FORMATS) == []
    assert parse_date(None, FORMATS, "foo") == ["foo"]
    assert parse_date("Sep 2023", FORMATS, "foo") == ["2023-09"]
    assert parse_date("2023-01", FORMATS, "foo") == ["2023-01"]
    assert parse_date("circa 2023", FORMATS, "foo") == ["2023"]
    assert parse_date("circa then", FORMATS, "foo") == ["foo"]
    assert parse_date("circa then", FORMATS) == ["circa then"]
    assert parse_date("23.5.", FORMATS) == ["23.5."]


def test_date_days_from_runtime():
    assert backdate(datetime(2023, 8, 3), 0) == "2023-08-03"
    assert backdate(datetime(2023, 8, 3), 182) == "2023-02-02"