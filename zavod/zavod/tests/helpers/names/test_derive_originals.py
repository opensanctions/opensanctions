from zavod.helpers.names import derive_original_values, Names


def test_derive_original_values_single_original():
    """When there's exactly one original value, all extracted values map to it."""
    original = Names(name="John/Jon Doe")
    extracted = Names(name=["John Doe"], alias="Jon Doe")

    result = derive_original_values(original, extracted)

    assert result == {
        "John Doe": "John/Jon Doe",
        "Jon Doe": "John/Jon Doe",
    }


def test_derive_original_values_exact_match():
    """When an extracted value exactly matches an original, no mapping is needed."""
    original = Names(name=["John/Jon .. Doe", "John Doe"])
    extracted = Names(name="John Doe", alias="Jon Doe")

    result = derive_original_values(original, extracted)

    # John Doe isn't contained exactly, and Jhon doe matches exactly so doesn't need an original_value
    assert result == {}


def test_derive_original_values_substring_match():
    """When an extracted value is contained in an original, it maps to that original."""
    original = Names(name="John Doe; Brandon Doe", alias="J. Doe")
    extracted = Names(name="John Brandon Doe", alias="Brandon Doe")

    result = derive_original_values(original, extracted)

    # John Brandon Doe isn't contained exactly. We're not getting more fancy with this.
    assert result == {
        "Brandon Doe": "John Doe; Brandon Doe",
    }


def test_derive_original_values_first_match_wins():
    """When multiple originals contain the extracted value, the first match is used."""
    original = Names(name=["John Brandon Doe", "John Smith"])
    extracted = Names(name="John")

    result = derive_original_values(original, extracted)

    # "John" is in both originals, but the first one should win
    assert result == {
        "John": "John Brandon Doe",
    }


def test_derive_original_values_no_match():
    """When no match is found, no mapping is created."""
    original = Names(name=["Johnn Doee", "Johnnn Doe"])
    extracted = Names(name="John Doe")

    result = derive_original_values(original, extracted)

    # No match found, so no mapping
    assert result == {}


def test_derive_original_values_empty_original():
    """When original is empty, no mappings are created."""
    original = Names()
    extracted = Names(name="John Doe")

    result = derive_original_values(original, extracted)

    assert result == {}


def test_derive_original_values_empty_extracted():
    """When extracted is empty, no mappings are created."""
    original = Names(name="John Doe")
    extracted = Names()

    result = derive_original_values(original, extracted)

    assert result == {}


def test_derive_original_values_cross_property_matching():
    """Original and extracted can be in different properties."""
    original = Names(alias="John Brandon Doe")
    extracted = Names(name="John", weakAlias="Brandon")

    result = derive_original_values(original, extracted)

    assert result == {
        "John": "John Brandon Doe",
        "Brandon": "John Brandon Doe",
    }


def test_derive_original_values_multiple_originals_different_props():
    """Multiple original values across different properties."""
    original = Names(name="John Doe", alias="J. Doe")
    extracted = Names(name="John", alias="J.")

    result = derive_original_values(original, extracted)

    # "John" is in "John Doe", "J." is in "J. Doe"
    assert result == {
        "John": "John Doe",
        "J.": "J. Doe",
    }


def test_derive_original_values_complex_scenario():
    """Complex scenario with multiple originals and extracted values."""
    original = Names(
        name=["John Brandon Doe", "Jane Smith"],
        alias="JBD",
    )
    extracted = Names(
        name=["Jane", "Brandon Doe"],
        alias="Brandon",
        weakAlias="Smith",
    )

    result = derive_original_values(original, extracted)

    # "John Doe" is in "John Brandon Doe"
    # "Jane" is in "Jane Smith"
    # "Brandon" is in "John Brandon Doe"
    # "Smith" is in "Jane Smith"
    assert result == {
        "Jane": "Jane Smith",
        "Brandon Doe": "John Brandon Doe",
        "Brandon": "John Brandon Doe",
        "Smith": "Jane Smith",
    }


def test_derive_original_values_single_original_multiple_props():
    """Single original value used for all extracted values across different props."""
    original = Names(name="John Brandon Doe")
    extracted = Names(
        name="John Doe",
        alias="Brandon",
        weakAlias="JBD",
    )

    result = derive_original_values(original, extracted)

    # All extracted values map to the single original
    assert result == {
        "John Doe": "John Brandon Doe",
        "Brandon": "John Brandon Doe",
        "JBD": "John Brandon Doe",
    }


def test_derive_original_values_partial_substring():
    """Test that only proper substrings are matched, not partial word matches."""
    original = Names(name="Smith Corporation")
    extracted = Names(name="Smith")

    result = derive_original_values(original, extracted)

    # "Smith" is contained in "Smith Corporation"
    assert result == {
        "Smith": "Smith Corporation",
    }
