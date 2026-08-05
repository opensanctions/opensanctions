import re

FEET_INCHES = re.compile(
    r"^\s*(?:(?P<feet>\d+(?:\.\d+)?)\s*(?:ft|'|\u2032))?"
    r"\s*(?:(?P<inches>\d+(?:\.\d+)?)\s*(?:in|\"|\u2033)?)?\s*$",
    re.IGNORECASE,
)

CM_IN = 2.54
IN_PER_FT = 12.0


def convert_height_to_cm(value: str | None) -> float | None:
    """Convert an American-style height expression to centimetres.

    Accepts feet and/or inches, e.g. ``"5' 11\\""``, ``"6ft 2in"``, ``"5ft"``,
    ``"5.5 ft"``, or a bare number (treated as centimetres, e.g. ``"180"`` or
    ``"180 cm"``).

    Args:
        value: The height string to convert.

    Returns:
        The height in centimetres, or ``None`` if the value cannot be parsed.
    """
    if value is None:
        return None
    text = value.strip().lower()
    if not text:
        return None
    bare = text.replace("cm", "").strip()
    if bare and bare.replace(".", "", 1).isdigit():
        return round(float(bare), 2)
    match = FEET_INCHES.match(text)
    if match is not None:
        feet = float(match.group("feet") or 0)
        inches = float(match.group("inches") or 0)
        cm = feet * IN_PER_FT * CM_IN + inches * CM_IN
        return round(cm, 2)
    return None
