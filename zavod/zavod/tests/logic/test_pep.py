from datetime import datetime

from zavod.logic.pep import backdate, occupancy_status



def test_backdate():
    assert backdate(datetime(2023, 8, 3), 0) == "2023-08-03"
    assert backdate(datetime(2023, 8, 3), 182) == "2023-02-02"