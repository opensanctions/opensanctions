from typing import TYPE_CHECKING
from typing import Optional, Generator, Tuple
from rigour.ids import get_identifier_format
from rigour.names import is_name
from prefixdate.precision import Precision
from followthemoney.types import registry
from followthemoney.property import Property

from zavod.logs import get_logger
from zavod.runtime.lookups import prop_lookup

if TYPE_CHECKING:
    from zavod.entity import Entity

VALIDATE_FORMATS = (
    "bic",
    "isin",
    "lei",
    "imo",
    "iban",
    "inn",
    "ogrn",
    "npi",
    "uei",
    "qid",
)
log = get_logger(__name__)


def clean_identifier(prop: Property, value: str) -> Optional[str]:
    normalized: Optional[str] = value
    if prop.format in VALIDATE_FORMATS:
        format_ = get_identifier_format(prop.format)
        normalized = format_.normalize(value)
    if normalized is None:
        log.warning(
            f"Failed to validate {prop.format} identifier: {value}",
            format=prop.format,
            prop=prop.name,
            value=value,
        )
        return value
    return normalized


def value_clean(
    entity: "Entity",
    prop: Property,
    value: Optional[str],
    cleaned: bool = False,
    fuzzy: bool = False,
    format: Optional[str] = None,
) -> Generator[Tuple[Property, str], None, None]:
    for prop_, item in prop_lookup(entity, prop, value):
        clean: Optional[str] = item
        if not cleaned:
            if prop_.type == registry.identifier:
                clean = clean_identifier(prop_, item)
            else:
                clean = prop_.type.clean_text(
                    item,
                    proxy=entity,
                    fuzzy=fuzzy,
                    format=format,
                )
        if prop_.type == registry.name and clean is not None:
            if entity.schema.is_a("LegalEntity") and not is_name(clean):
                log.warning(
                    f"Property value {prop_.name!r} is not a valid name: {value}",
                    entity_id=entity.id,
                    value=value,
                    clean=clean,
                )
                continue
        if prop_.type == registry.date and clean is not None:
            # none of the information in OpenSanctions is time-critical
            clean = clean[: Precision.DAY.value]
        if clean is not None:
            if len(clean) > prop_.max_length:
                log.warning(
                    f"Property value for {prop_.name} exceeds type length: {value}",
                    entity_id=entity.id,
                    prop=prop_.name,
                    value=value,
                    clean=clean,
                )
                # clean = clean[: prop.type.max_length]

            yield prop_, clean
            continue
        if prop_.type == registry.phone:
            # Do not have capacity to clean all phone numbers, allow broken ones
            yield prop_, item
            continue
        log.warning(
            f"Rejected property value [{prop_.name}]: {value}",
            entity_id=entity.id,
            prop=prop_.name,
            value=value,
        )
