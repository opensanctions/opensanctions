from zavod.helpers.text import multi_split, remove_bracketed, is_empty, clean_note


def test_multi_split():
    # Basic null/empty handling
    assert len(multi_split(None, ["a)"])) == 0
    assert len(multi_split("", ["a)"])) == 0

    # No split occurs
    text = "banana"
    assert len(multi_split(text, ["a)"])) == 1

    # Basic splitting
    text = "a) banana b) peach"
    assert len(multi_split(text, ["a)", "b)"])) == 2
    assert multi_split(text, ["a)", "b)"]) == ["banana", "peach"]

    # List input with None
    text = ["a) banana b) peach", None]
    assert len(multi_split(text, ["a)", "b)"])) == 2

    # Test that splitter order matters (substring issue)
    # If "i)" is split first, then "ii)" becomes "i)" which breaks
    text = "i) first ii) second iii) third"
    # Correct order: longer splitters first
    result_correct = multi_split(text, ["iii)", "ii)", "i)"])
    assert result_correct == ["first", "second", "third"]

    # Wrong order will be flagged but still processed
    result_wrong = multi_split(text, ["i)", "ii)", "iii)"])
    # The results differ, which is why validation exists
    assert result_wrong != result_correct

    # Test multiple delimiters
    text = "apple,banana/cherry;date"
    result = multi_split(text, [",", "/", ";"])
    assert result == ["apple", "banana", "cherry", "date"]

    # Test with whitespace-only fragments being filtered out
    text = "a)  b) something c)  "
    result = multi_split(text, ["a)", "b)", "c)"])
    assert result == ["something"]

    # Test that None values in fragments are filtered
    text = ["a) banana", None, "b) peach"]
    result = multi_split(text, ["a)", "b)"])
    assert result == ["banana", "peach"]


def test_remove_bracketed():
    assert remove_bracketed(None) is None
    text = "banana"
    assert remove_bracketed(text) == text
    text = "banana (with peaches)"
    out = remove_bracketed(text)
    assert out, text
    assert out.strip() == "banana"


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
