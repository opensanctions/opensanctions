from datetime import datetime
import pytest
import requests
import requests_mock

from zavod.logic.pep import (
    PositionCategorisation,
    backdate,
    occupancy_status,
    OccupancyStatus,
    categorise,
)
from zavod.meta import Dataset
from zavod.context import Context
from zavod.helpers.positions import make_position


def test_backdate():
    assert backdate(datetime(2023, 8, 3), 0) == "2023-08-03"
    assert backdate(datetime(2023, 8, 3), 182) == "2023-02-02"


def test_occupancy_status(testdataset1: Dataset):
    context = Context(testdataset1)
    person = context.make("Person")
    person.id = "thabo"

    def status(implies, start, end, birth=None, death=None, position_topics=[]):
        pos = make_position(
            context, name="A position", country="ls", topics=position_topics
        )
        return occupancy_status(
            context,
            person,
            pos,
            implies,
            datetime(2021, 1, 1),
            start,
            end,
            birth,
            death,
        )

    # Current when no end implies current
    assert status(True, "2020-01-01", None) == OccupancyStatus.CURRENT
    # Even with no start date
    assert status(True, None, None) == OccupancyStatus.CURRENT

    # Not a PEP with no dates and no end doesn't imply current.
    assert status(False, None, None, None, None) is None

    # Current when end date is in the future
    # (even when it started longer than MAX_OFFICE ago)
    assert status(True, "1950-01-01", "2021-01-02") == OccupancyStatus.CURRENT
    # Ended when end date is in the past
    assert status(True, "1950-01-01", "2020-12-31") == OccupancyStatus.ENDED
    # Not a PEP when end date is longer than DEFAULT_AFTER_OFFICE ago
    assert status(False, "1950-01-01", "2016-01-01") is None
    # Still a PEP when end_date is longer than DEFAULT_AFTER_OFFICE
    # but not longer than EXTENDED_AFTER_OFFICE ago
    assert (
        status(False, "1950-01-01", "2016-01-01", position_topics=["gov.national"])
        is OccupancyStatus.ENDED
    )
    # Not a PEP when end date is longer than NATIONAL_AFTER_OFFICE ago
    assert (
        status(False, "1950-01-01", "2001-01-01", position_topics=["gov.national"])
        is None
    )
    categorisation_override = occupancy_status(
        context,
        person,
        make_position(context, "Pos", country="ls"),
        start_date="1950-01-01",
        end_date="2016-12-31",
        categorisation=PositionCategorisation(["gov.national"], True),
    )
    # Still a PEP within NATIONAL_AFTER_OFFICE indicated by categorisation topics.
    assert categorisation_override is OccupancyStatus.ENDED

    # Not a PEP when end date is unknown but start date > MAX_OFFICE
    assert status(False, "1981-01-01", None) is None
    assert status(True, "1981-01-01", None) is None
    assert status(True, "1981-01-01", None) is None

    # Unknown when started really long ago but < MAX_OFFICE ago
    # Note this is not counting leap days
    assert status(False, "1981-01-15", None) == OccupancyStatus.UNKNOWN
    # Current when the source is really good (no end implies current)
    assert status(True, "1981-01-15", None) == OccupancyStatus.CURRENT

    # Not a PEP if they died longer than AFTER_DEATH ago
    assert status(True, "2020-01-01", None, None, "2016-01-01") is None
    assert status(True, "1950-01-01", "2021-01-02", None, "2016-01-01") is None
    assert status(True, "1950-01-01", "2020-12-31", None, "2016-01-01") is None

    # Not a PEP if they were born longer than MAX_AGE ago
    assert status(True, "2020-01-01", None, "1910-01-01") is None
    assert status(True, "1950-01-01", "2021-01-02", "1910-01-01") is None
    assert status(True, "1950-01-01", "2020-12-31", "1910-01-01") is None


def test_categorise_new(testdataset1: Dataset):
    context = Context(testdataset1)
    position = make_position(context, "A position", country="ls")

    data = {
        "entity_id": position.id,
        "caption": position.caption,
        "countries": ["ls"],
        "topics": [],
        "is_pep": None,
    }
    with requests_mock.Mocker() as m:
        m.get(
            "/positions/osv-40a302b7f09ea065880a3c840855681b18ead5a4", status_code=404
        )
        m.post("/positions/", status_code=201, json=data)
        categorisation = categorise(context, position)

    assert categorisation.is_pep is None
    assert categorisation.topics == []


def test_categorise_existing(testdataset1: Dataset):
    context = Context(testdataset1)
    position = make_position(context, "Another position", country="ls")

    data = {
        "entity_id": position.id,
        "caption": position.caption,
        "countries": ["ls"],
        "topics": ["gov.igo"],
        "is_pep": True,
    }
    with requests_mock.Mocker() as m:
        m.get(
            "/positions/osv-3c5b98b2f63e39d0a341af03d2dde959e25f07f8",
            status_code=200,
            json=data,
        )
        categorisation = categorise(context, position)

    assert categorisation.is_pep is True
    assert categorisation.topics == ["gov.igo"]


def test_categorise_unauthorised(testdataset1: Dataset):
    context = Context(testdataset1)
    position = make_position(
        context, "Another position", country="ls", topics=["gov.igo"]
    )

    with requests_mock.Mocker() as m:
        m.get(f"/positions/{position.id}", status_code=404)
        m.post("/positions/", status_code=401)
        with pytest.raises(requests.exceptions.HTTPError) as exc:
            categorise(context, position)
        assert exc.value.response.status_code == 401

