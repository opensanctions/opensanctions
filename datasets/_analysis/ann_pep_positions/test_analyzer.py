from collections import defaultdict

from zavod import Entity, Dataset
from .analyzer import build_consolidated_influence_labels, get_best_occupancy_status


def test_influence() -> None:
    dataset = Dataset({"name": "test", "title": "test"})

    def o(statuses: list[str]) -> Entity:
        data = {
            "schema": "Occupancy",
            "id": "occupancy-id",
            "properties": {"status": statuses},
        }
        return Entity.from_data(dataset, data)

    # Current trumps all
    # Both for different occupancies for same influence
    # And merged occupancies with two statuses
    influence = defaultdict(set)
    influence["gov.national"].add(get_best_occupancy_status(o(["unknown"])))
    influence["gov.national"].add(get_best_occupancy_status(o(["current"])))
    assert build_consolidated_influence_labels(influence) == [
        "National government (current)"
    ]

    influence = defaultdict(set)
    influence["gov.national"].add(get_best_occupancy_status(o(["unknown", "current"])))
    assert build_consolidated_influence_labels(influence) == [
        "National government (current)"
    ]

    influence = defaultdict(set)
    influence["gov.national"].add(get_best_occupancy_status(o(["current", "unknown"])))
    assert build_consolidated_influence_labels(influence) == [
        "National government (current)"
    ]

    influence = defaultdict(set)
    influence["gov.national"].add(get_best_occupancy_status(o(["current"])))
    influence["gov.national"].add(get_best_occupancy_status(o(["ended"])))
    assert build_consolidated_influence_labels(influence) == [
        "National government (current)"
    ]

    # Different occupancies for same influence
    # Unknown trumps ended
    influence = defaultdict(set)
    influence["gov.national"].add(get_best_occupancy_status(o(["ended"])))
    influence["gov.national"].add(get_best_occupancy_status(o(["unknown"])))
    assert build_consolidated_influence_labels(influence) == [
        "National government (unknown status)"
    ]
    influence = defaultdict(set)
    influence["gov.national"].add(get_best_occupancy_status(o(["unknown"])))
    influence["gov.national"].add(get_best_occupancy_status(o(["ended"])))
    assert build_consolidated_influence_labels(influence) == [
        "National government (unknown status)"
    ]

    # Same occupancy, ended trumps unknown
    influence = defaultdict(set)
    influence["gov.national"].add(get_best_occupancy_status(o(["ended", "unknown"])))
    assert build_consolidated_influence_labels(influence) == [
        "National government (past)"
    ]
    influence = defaultdict(set)
    influence["gov.national"].add(get_best_occupancy_status(o(["unknown", "ended"])))
    assert build_consolidated_influence_labels(influence) == [
        "National government (past)"
    ]
