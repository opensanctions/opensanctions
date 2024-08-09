from zavod.entity import Entity
from zavod.meta.dataset import Dataset
from zavod.helpers.dates import parse_date, check_no_year, extract_years
from zavod.helpers.dates import replace_months, apply_date, apply_dates

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


def test_replace_months(testdataset1: Dataset):
    assert replace_months(testdataset1, "3. März 2021") == "3. mar 2021"
    assert replace_months(testdataset1, "3. März2021") == "3. März2021"


def test_apply_date(testdataset1: Dataset):
    data = {"id": "doe", "schema": "Person", "properties": {"name": ["John Doe"]}}
    person = Entity(testdataset1, data)
    apply_date(person, "birthDate", "2024-01-23")
    assert "2024-01-23" in person.pop("birthDate")
    apply_date(person, "birthDate", "14. März 2021")
    assert "2021-03-14" in person.pop("birthDate")

    apply_date(person, "birthDate", "banana")
    assert "banana" not in person.pop("birthDate")

    apply_dates(person, "birthDate", ["banana"])
    assert "banana" not in person.pop("birthDate")

    testdataset1.dates.year_only = False
    apply_dates(person, "birthDate", ["circa 2024"])
    assert "2024" not in person.pop("birthDate")

    testdataset1.dates.year_only = True
    apply_dates(person, "birthDate", ["circa 2024"])
    assert "2024" in person.pop("birthDate")
    testdataset1.dates.year_only = False
