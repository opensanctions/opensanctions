from zavod.parse.text import multi_split, remove_bracketed, is_empty, clean_note


def test_multi_split():
    assert len(multi_split(None, ["a)"])) == 0
    assert len(multi_split("", ["a)"])) == 0
    text = "banana"
    assert len(multi_split(text, ["a)"])) == 1
    text = "a) banana b) peach"
    assert len(multi_split(text, ["a)", "b)"])) == 2
    text = ["a) banana b) peach", None]
    assert len(multi_split(text, ["a)", "b)"])) == 2


def test_remove_bracketed():
    assert remove_bracketed(None) is None
    text = "banana"
    assert remove_bracketed(text) == text
    text = "banana (with peaches)"
    assert remove_bracketed(text).strip() == "banana"


def test_is_empty():
    assert is_empty(None) is True
    assert is_empty("") is True
    assert is_empty("  ") is True
    assert is_empty(" hello ") is False
    assert is_empty(5) is False


def test_clean_note():
    assert clean_note([]) == []
    assert clean_note(None) == []
    assert clean_note(["hello"]) == ["hello"]
    assert clean_note(["hello", None]) == ["hello"]
