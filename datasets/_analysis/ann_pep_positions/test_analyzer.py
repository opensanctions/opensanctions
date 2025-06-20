from .analyzer import Influence


def test_influence():
    # Current trumps all
    # Both for different occupancies for same influence
    # And merged occupancies with two statuses
    influence = Influence()
    influence.add({"gov.national"}, ["unknown"])
    influence.add({"gov.national"}, ["current"])
    assert influence.make_keywords() == ["National government (current)"]

    influence = Influence()
    influence.add({"gov.national"}, ["unknown", "current"])
    assert influence.make_keywords() == ["National government (current)"]

    influence = Influence()
    influence.add({"gov.national"}, ["current", "unknown"])
    assert influence.make_keywords() == ["National government (current)"]

    influence = Influence()
    influence.add({"gov.national"}, ["current"])
    influence.add({"gov.national"}, ["ended"])
    assert influence.make_keywords() == ["National government (current)"]

    # Different occupancies for same influence
    # Unknown trumps ended
    influence = Influence()
    influence.add({"gov.national"}, ["ended"])
    influence.add({"gov.national"}, ["unknown"])
    assert influence.make_keywords() == ["National government (unknown status)"]
    influence = Influence()
    influence.add({"gov.national"}, ["unknown"])
    influence.add({"gov.national"}, ["ended"])
    assert influence.make_keywords() == ["National government (unknown status)"]

    # Same occupancy, ended trumps unknown
    influence = Influence()
    influence.add({"gov.national"}, ["ended", "unknown"])
    assert influence.make_keywords() == ["National government (past)"]
    influence = Influence()
    influence.add({"gov.national"}, ["unknown", "ended"])
    assert influence.make_keywords() == ["National government (past)"]
