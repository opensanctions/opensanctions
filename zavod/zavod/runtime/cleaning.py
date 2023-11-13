from stdnum import bic, isin  # type: ignore
from stdnum.exceptions import ValidationError  # type: ignore
from typing import TYPE_CHECKING
from typing import Optional, Generator, Tuple
from prefixdate.precision import Precision
from followthemoney.types import registry
from followthemoney.property import Property

from zavod.logs import get_logger
from zavod.runtime.lookups import type_lookup

if TYPE_CHECKING:
    from zavod.entity import Entity

log = get_logger(__name__)


def normalize_bic(value: str) -> Optional[str]:
    # Examples: SABRRUMMXXX = SABRRUMM
    try:
        bic_value: Optional[str] = bic.validate(value)
        if bic_value is None:
            return None
        return bic_value[:8]
    except ValidationError:
        return None


def normalize_isin(value: str) -> Optional[str]:
    try:
        isin_value: Optional[str] = isin.validate(value)
        if isin_value is None:
            return None
        return isin_value
    except ValidationError:
        return None


def clean_identifier(
    entity: "Entity", prop: Property, value: str
) -> Tuple[Property, str]:
    if prop.name == "swiftBic":
        value = normalize_bic(value) or value
    if prop.name in ("isin", "isinCode"):
        value = normalize_isin(value) or value
    return prop, value


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
                prop_, clean = clean_identifier(entity, prop, clean)
            if prop.type == registry.date:
                # none of the information in OpenSanctions is time-critical
                clean = clean[: Precision.DAY.value]
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
