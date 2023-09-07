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
    pos = make_position(context, name="A position", country="ls")
    person = context.make("Person")
    person.id = "thabo"

    def make(implies, start, end):
        return occupancy_status(
            context, person, pos, implies, datetime(2021, 1, 1), start, end
        )

    current_no_end = make(True, "2020-01-01", None)
    assert current_no_end == OccupancyStatus.CURRENT

    ended_no_start = make(True, None, "2020-01-01")
    assert ended_no_start == OccupancyStatus.ENDED

    current_with_end = make(True, "1950-01-01", "2021-01-02")
    assert current_with_end == OccupancyStatus.CURRENT

    ended = make(True, "1950-01-01", "2020-12-31")
    assert ended == OccupancyStatus.ENDED

    # > MAX_OFFICE is not a PEP
    beyond_max_office = make(False, "1981-01-01", None)
    assert beyond_max_office == None

    # < MAX_OFFICE is a PEP but current status is unknown
    # Note this is not counting leap days
    within_max_office = make(False, "1981-01-15", None)
    assert within_max_office == OccupancyStatus.UNKNOWN

    none = make(False, "1950-01-01", "2015-01-01")
    assert none is None
