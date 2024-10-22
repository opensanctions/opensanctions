from typing import TYPE_CHECKING
from typing import Optional, Generator, Tuple
from rigour.ids import ISIN, BIC, LEI, IBAN, IMO
from prefixdate.precision import Precision
from followthemoney.types import registry
from followthemoney.property import Property

from zavod.logs import get_logger
from zavod.runtime.lookups import type_lookup

if TYPE_CHECKING:
    from zavod.entity import Entity

log = get_logger(__name__)


def clean_identifier(prop: Property, value: str) -> Tuple[Property, Optional[str]]:
    normalized: Optional[str] = value
    if prop.format == "bic":
        normalized = BIC.normalize(value)
    if prop.format == "isin":
        normalized = ISIN.normalize(value)
    if prop.format == "lei":
        normalized = LEI.normalize(value)
    if prop.format == "imo":
        normalized = IMO.normalize(value)
    if prop.format == "iban":
        normalized = IBAN.normalize(value)
    if normalized is None:
        log.warning(
            "Failed to validate identifier",
            format=prop.format,
            prop=prop.name,
            value=value,
        )
        return prop, value
    return prop, normalized


def value_clean(
    entity: "Entity",
    prop: Property,
    value: Optional[str],
    cleaned: bool = False,
    fuzzy: bool = False,
    format: Optional[str] = None,
) -> Generator[Tuple[Property, str], None, None]:
    for item in type_lookup(entity.dataset, prop.type, value):
        clean: Optional[str] = item
        if not cleaned:
            clean = prop.type.clean_text(
                item,
                proxy=entity,
                fuzzy=fuzzy,
                format=format,
            )
        if clean is not None:
            prop_ = prop
            if prop.type == registry.identifier:
                prop_, clean = clean_identifier(prop, clean)
            if prop.type == registry.date:
                # none of the information in OpenSanctions is time-critical
                clean = clean[: Precision.DAY.value]

            if len(clean) > prop.max_length:
                log.warning(
                    "Property value exceeds type length",
                    entity_id=entity.id,
                    prop=prop.name,
                    value=value,
                    clean=clean,
                )
                # clean = clean[: prop.type.max_length]

            yield prop_, clean
            continue
        if prop.type == registry.phone:
            # Do not have capacity to clean all phone numbers, allow broken ones
            yield prop, item
            continue
        log.warning(
            "Rejected property value",
            entity_id=entity.id,
            prop=prop.name,
            value=value,
        )
