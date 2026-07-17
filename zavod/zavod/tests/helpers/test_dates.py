import logging
from datetime import datetime, timedelta, UTC

import pytest
from structlog.testing import capture_logs

from zavod.context import Context
from zavod.entity import Entity
from zavod.meta.dataset import Dataset
from zavod.helpers.dates import extract_years, extract_date, backdate
from zavod.helpers.dates import replace_months, apply_date, apply_dates
from zavod.helpers.dates import within_max_age
from zavod.settings import RUN_TIME

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


def test_extract_date_two_digit_year(
    testdataset1: Dataset, caplog: pytest.LogCaptureFixture
) -> None:
    # The base date selects the century.
    assert extract_date(
        testdataset1,
        "16-07-68",
        formats=("%d-%m-%y",),
        two_digit_year_base=1926,
    ) == ["1968-07-16"]
    assert extract_date(
        testdataset1,
        "16-07-68",
        formats=("%d-%m-%y",),
        two_digit_year_base=2000,
    ) == ["2068-07-16"]

    # Without a base year, the fixed strptime window applies and prefixdate warns.
    # The warning reaches the dataset issue log through the standard logging chain.
    with caplog.at_level(logging.WARNING, logger="prefixdate.formats"):
        assert extract_date(testdataset1, "23-10-64", formats=("%d-%m-%y",)) == [
            "2064-10-23"
        ]
    assert "two-digit year format" in caplog.text, caplog.text


def test_apply_date_two_digit_year(testdataset1: Dataset):
    data = {"id": "doe", "schema": "Person", "properties": {"name": ["John Doe"]}}
    person = Entity(testdataset1, data)
    apply_date(
        person,
        "birthDate",
        "16-07-68",
        formats=("%d-%m-%y",),
        two_digit_year_base=1926,
    )
    assert person.pop("birthDate") == ["1968-07-16"]

    apply_dates(
        person,
        "birthDate",
        ["16-07-68", "23-10-64"],
        formats=("%d-%m-%y",),
        two_digit_year_base=1926,
    )
    assert sorted(person.pop("birthDate")) == ["1964-10-23", "1968-07-16"]


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
    bd = now.astimezone(UTC).date().isoformat()
    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", now)
    assert bd in person.pop("birthDate")
    assert cap_logs == [], cap_logs

    # date
    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", now.date())
    assert bd in person.pop("birthDate")
    assert cap_logs == [], cap_logs

    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", 25722)  # type: ignore
        # apply_date(person, "birthDate", 2572)  # type: ignore
    assert not len(person.pop("birthDate"))
    assert len(cap_logs) == 1, cap_logs
    assert cap_logs[0]["prop"] == "birthDate", cap_logs


def test_backdate():
    assert backdate(datetime(2023, 8, 3), timedelta(days=0)) == "2023-08-03"
    assert backdate(datetime(2023, 8, 3), timedelta(days=182)) == "2023-02-02"


def test_within_max_age(vcontext: Context):
    assert within_max_age(vcontext, RUN_TIME.date().isoformat())
    # A year-precision date whose year straddles the cutoff may be as late as
    # Dec 31 of that year, so it stays within the age window.
    cutoff_year = (RUN_TIME - timedelta(days=5 * 365)).year
    assert within_max_age(vcontext, str(cutoff_year))
    # The year before the cutoff year has fully elapsed.
    assert not within_max_age(vcontext, str(cutoff_year - 1))
    assert not within_max_age(vcontext, "1999-01-01")


def test_apply_date_future_birth_date(testdataset1: Dataset):
    data = {"id": "doe", "schema": "Person", "properties": {"name": ["John Doe"]}}
    person = Entity(testdataset1, data)

    # strptime's %y pivot maps 00-68 to 20xx: a 1968 birth date parses into
    # the future and must not be stored.
    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", "16-07-68", formats=("%d-%m-%y",))
    assert person.get("birthDate") == []
    assert len(cap_logs) == 1, cap_logs
    assert cap_logs[0]["prop"] == "birthDate", cap_logs

    # Two-digit years 69-99 pivot to 19xx and are kept.
    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", "16-07-71", formats=("%d-%m-%y",))
    assert "1971-07-16" in person.pop("birthDate")
    assert cap_logs == [], cap_logs

    # Explicit future dates are rejected too.
    with capture_logs() as cap_logs:
        apply_date(person, "birthDate", "2999-01-01")
    assert person.get("birthDate") == []
    assert len(cap_logs) == 1, cap_logs

    # The guard applies only to birth dates.
    with capture_logs() as cap_logs:
        apply_date(person, "deathDate", "2999-01-01")
    assert "2999-01-01" in person.pop("deathDate")
    assert cap_logs == [], cap_logs
