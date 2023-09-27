from datetime import datetime

from zavod.logic.pep import backdate, occupancy_status, OccupancyStatus
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
    # but not longer than NATIONAL_AFTER_OFFICE ago
    assert (
        status(False, "1950-01-01", "2016-01-01", position_topics=["gov.national"])
        is OccupancyStatus.ENDED
    )
    # Not a PEP when end date is longer than NATIONAL_AFTER_OFFICE ago
    assert (
        status(False, "1950-01-01", "2001-01-01", position_topics=["gov.national"])
        is None
    )

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
