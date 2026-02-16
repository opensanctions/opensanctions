import re
from typing import TYPE_CHECKING

from followthemoney import Property

from zavod.logs import get_logger
from zavod.runtime.lookups import is_type_lookup_value

if TYPE_CHECKING:
    from zavod.entity import Entity


log = get_logger(__name__)

HTML_ENTITY_PATTERN = re.compile(
    r"&(?:"
    r"#[0-9]{1,7};|"  # Decimal: &#65;
    r"#[xX][0-9a-fA-F]{1,6};"  # Hex: &#x41; &#X41;
    # Disabled for now because it produces too many false positives
    # r"|[a-zA-Z][a-zA-Z0-9]*;?"  # Named: &amp; &lt (HTML5 allows no semicolon for some)
    r")"
)

XSS_SUSPECT_PATTERN = re.compile(
    r"<[^>]*>|"  # Tags
    r"javascript:|data:|vbscript:|"  # URI schemes
    r"on\w+\s*=|"  # Event handlers
    # Disabled for now because it produces too many false positives
    # r"&#|&[a-zA-Z]",  # Entity references
    r"&#[a-zA-Z]",  # Entity references
    re.IGNORECASE,
)


def check_xss_html_smell(entity: "Entity", prop: Property, value: str) -> str | None:
    """A very basic HTML entity and XSS check.

    This validator does not guarantee that the value is "safe" to render in any context,
    but is more of a smell detector to find values where we screwed up on data extraction
    or that are crude attempts at planting something malicious in our data.

    Things that are results of lookups (i.e. that we manually reviewed) are exempt from this check.
    To make a string exempt, just add a lookup from "value" -> "value" to the dataset metadata.

    A full-blown XSS filter (that guarantees that a value is safe to render in a certain HTML context)
    would also escape things that we require to remain intact for matching purposes.
    """
    has_xss_smell = XSS_SUSPECT_PATTERN.search(value)
    has_html_entities = HTML_ENTITY_PATTERN.search(value)
    if not has_xss_smell and not has_html_entities:
        return value

    # Values that came out of a lookup (i.e. that we manually reviewed) are exempt from this check.
    if is_type_lookup_value(entity, prop.type, value):
        return value

    log.warning(
        f"HTML/XSS suspicion in property value: {value}",
        prop=prop.name,
        value=value,
    )
    # FIXME: I want to run this in warning mode first, later we need to enable this:
    # Remove for safety unless it's set
    # return None

    return value
