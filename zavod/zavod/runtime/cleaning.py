from rigour.ids import ISIN, BIC
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


def clean_identifier(prop: Property, value: str) -> Tuple[Property, str]:
    if prop.name == "swiftBic":
        value = BIC.normalize(value) or value
    if prop.name in ("isin", "isinCode"):
        value = ISIN.normalize(value) or value
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
