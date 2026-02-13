from zavod.extract.names.clean import Names


def test_names_equality():
    assert Names(name="John Doe") == Names(name="John Doe")
    assert Names(name="John Doe") != Names(name="Jane Doe")
    assert Names(name="John Doe") == Names(name=["John Doe"])
    assert Names(name=["A", "B"]) == Names(name=["A", "B"])
    assert Names(name=["A", "B"]) == Names(name=["B", "A"])
    assert Names(name=["A"]) != Names(alias=["A"])
