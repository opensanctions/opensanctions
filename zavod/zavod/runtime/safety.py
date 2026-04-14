import re
from typing import TYPE_CHECKING, Optional

from followthemoney import registry, Property

from zavod.logs import get_logger
from zavod.runtime.lookups import get_type_lookup_silence_warnings, is_type_lookup_value


if TYPE_CHECKING:
    from zavod.entity import Entity


log = get_logger(__name__)

SKIP_TYPES = (registry.url, registry.entity)

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
    r"(\b)on\w+\s*=|"  # Event handlers
    # Disabled for now because it produces too many false positives
    # r"&#|&[a-zA-Z]",  # Entity references
    r"&#[a-zA-Z]",  # Entity references
    re.IGNORECASE,
)

# lookups:
#   type.text:
#     options:
#       - match:
#         - "goes by the alias <gg>"
#       - silence_warnings: [xss-html-smell]
SILENCE_WARNING_TYPE = "xss-html-smell"


def check_xss_html_smell(
    entity: "Entity", prop: Property, *, raw_value: Optional[str], cleaned_value: str
) -> str | None:
    """A very basic HTML entity and XSS check.

    This validator does not guarantee that the value is "safe" to render in any context,
    but is more of a smell detector to find values where we screwed up on data extraction
    or that are crude attempts at planting something malicious in our data.

    Things that are results of lookups (i.e. that we manually reviewed) are exempt from this check.
    To make a string exempt, just add a lookup from "value" -> "value" to the dataset metadata.

    A full-blown XSS filter (that guarantees that a value is safe to render in a certain HTML context)
    would also escape things that we require to remain intact for matching purposes.
    """
    if prop.type in SKIP_TYPES:
        # URLs are often the source of HTML entities and also often contain characters that
        # look like XSS attempts, but are not.
        return cleaned_value
    has_xss_smell = XSS_SUSPECT_PATTERN.search(cleaned_value)
    has_html_entities = HTML_ENTITY_PATTERN.search(cleaned_value)
    if not has_xss_smell and not has_html_entities:
        return cleaned_value

    # Allow settings silence_warnings: [xss-html-smell] for certain values
    if SILENCE_WARNING_TYPE in get_type_lookup_silence_warnings(
        entity, prop.type, cleaned_value
    ):
        return cleaned_value

    # TODO: Phase this out in favor of silence_warnings
    if is_type_lookup_value(entity, prop.type, cleaned_value):
        return cleaned_value

    log.warning(
        f"HTML/XSS suspicion in property value: {cleaned_value}",
        entity_id=entity.id,
        prop=prop.name,
        prop_type=prop.type.name,
        raw_value=raw_value,
        cleaned_value=cleaned_value,
    )
    # FIXME: I want to run this in warning mode first, later we need to enable this:
    # Remove for safety unless it's set
    # return None

    return cleaned_value
