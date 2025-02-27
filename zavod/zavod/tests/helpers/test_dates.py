from datetime import datetime, timezone
from structlog.testing import capture_logs

from zavod.entity import Entity
from zavod.meta.dataset import Dataset
from zavod.helpers.dates import extract_years, extract_date
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


def test_extract_date(testdataset1: Dataset):
    assert extract_date(testdataset1, "foo") == ["foo"]
    assert extract_date(testdataset1, "2. mar 2023") == ["2023-03-02"]
    assert extract_date(testdataset1, "2. März 2023") == ["2023-03-02"]

    # Check always-accepted formats
    assert "%Y-%m" not in testdataset1.dates.formats
    assert extract_date(testdataset1, "2023-01") == ["2023-01"]


def test_replace_months(testdataset1: Dataset):
    assert replace_months(testdataset1, "3. März 2021") == "3. mar 2021"
    assert replace_months(testdataset1, "3. März2021") == "3. März2021"


def test_apply_date(testdataset1: Dataset):
    data = {"id": "doe", "schema": "Person", "properties": {"name": ["John Doe"]}}
    person = Entity(testdataset1, data)

    # None

    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", None)
    assert not len(person.get("birthDate"))
    assert cap_logs == [], cap_logs

    # Good dates

    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", "2024-01-23")
    assert "2024-01-23" in person.pop("birthDate")
    assert cap_logs == [], cap_logs

    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", "14. März 2021")
    assert "2021-03-14" in person.pop("birthDate")
    assert cap_logs == [], cap_logs

    # banana

    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", "banana")
    assert "banana" not in person.pop("birthDate")
    assert len(cap_logs) == 1, cap_logs
    assert cap_logs[0]["prop"] == "birthDate", cap_logs

    with capture_logs() as cap_logs:
        apply_dates(person, "birthDate", ["banana"])
    assert "banana" not in person.pop("birthDate")
    assert len(cap_logs) == 1, cap_logs
    assert cap_logs[0]["prop"] == "birthDate", cap_logs

    # Year only

    testdataset1.dates.year_only = False
    with capture_logs() as cap_logs:
        apply_dates(person, "birthDate", ["ca 2024"])
    assert "2024" not in person.pop("birthDate")
    assert len(cap_logs) == 1, cap_logs
    assert cap_logs[0]["prop"] == "birthDate", cap_logs

    testdataset1.dates.year_only = True
    with capture_logs() as cap_logs:
        apply_dates(person, "birthDate", ["circa 2024"])
    assert "2024" in person.pop("birthDate")
    testdataset1.dates.year_only = False
    assert cap_logs == [], cap_logs

    # datetime

    now = datetime.now()
    bd = now.astimezone(timezone.utc).date().isoformat()
    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", now)
    assert bd in person.pop("birthDate")
    assert cap_logs == [], cap_logs

    # date

    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", now.date())
    assert bd in person.pop("birthDate")
    assert cap_logs == [], cap_logs
