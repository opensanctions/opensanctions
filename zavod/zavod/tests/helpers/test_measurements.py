import pytest

from zavod.helpers.measurements import convert_height_to_cm


@pytest.mark.parametrize(
    "value,expected",
    [
        ("5' 11\"", 180.34),
        ("5'11\"", 180.34),
        ("6ft 2in", 187.96),
        ("6ft2in", 187.96),
        ("5ft", 152.4),
        ("5'", 152.4),
        ("6 ft 2 in", 187.96),
        ("5.5 ft", 167.64),
        ("5' 11", 180.34),
        ("180", 180.0),
        ("180 cm", 180.0),
    ],
)
def test_convert_height_to_cm(value: str, expected: float) -> None:
    assert convert_height_to_cm(value) == pytest.approx(expected)


@pytest.mark.parametrize(
    "value",
    ["", "unknown", "tall", "5x", "--", None],
)
def test_convert_height_to_cm_invalid(value: str | None) -> None:
    assert convert_height_to_cm(value) is None
