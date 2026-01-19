import re
from typing import TYPE_CHECKING

from followthemoney import Property

from zavod.logs import get_logger
from zavod.runtime.lookups import is_lookup_value

if TYPE_CHECKING:
    from zavod.entity import Entity


log = get_logger(__name__)

HTML_ENTITY_PATTERN = re.compile(
    r"&(?:"
    r"#[0-9]{1,7};|"  # Decimal: &#65;
    r"#[xX][0-9a-fA-F]{1,6};|"  # Hex: &#x41; &#X41;
    r"[a-zA-Z][a-zA-Z0-9]*;?"  # Named: &amp; &lt (HTML5 allows no semicolon for some)
    r")"
)

XSS_SUSPECT_PATTERN = re.compile(
    r"<[^>]*>|"  # Tags
    r"javascript:|data:|vbscript:|"  # URI schemes
    r"on\w+\s*=|"  # Event handlers
    r"&#|&[a-zA-Z]",  # Entity references
    re.IGNORECASE,
)


def check_xss(entity: Entity, prop: Property, value: str) -> str | None:
    """A very basic HTML entity and XSS check to prevent script injections."""
    # This is very .... rustic. The reason for that design is that there's a lot of things
    # here we do not want to do, which normal XSS filters would do, like stripping tags
    # or escaping entities. Both of these would render our data useless for matching purposes.
    # So we only want to identify the most egregious cases and log them for manual review.
    has_xss = XSS_SUSPECT_PATTERN.search(value)
    has_entities = HTML_ENTITY_PATTERN.search(value)
    if not has_xss and not has_entities:
        return value
    if is_lookup_value(entity, prop.type, value):
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
