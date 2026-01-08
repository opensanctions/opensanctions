import unicodedata
from typing import TYPE_CHECKING
from typing import Optional, Generator, Tuple
from rigour.ids import get_identifier_format
from rigour.names import is_name
from prefixdate.precision import Precision
from followthemoney import registry, Property, model
from followthemoney.types import PropertyType

from zavod.logs import get_logger
from zavod.runtime.lookups import prop_lookup, get_type_lookup


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
    "uscc",
)

# These are not type name properties for reasons described in
# https://github.com/opensanctions/followthemoney/blob/cf0bdb11146a6eaf2de8a1ecba43683432259ffe/followthemoney/schema/Person.yaml#L28
# (TLDR: they're string types because we don't want to match on them and we don't have
# a NamePart type yet, see https://github.com/opensanctions/followthemoney/issues/71)
VALIDATE_AS_NAME_PROPS = {
    model.get_qname("Person:firstName"),
    model.get_qname("Person:secondName"),
    model.get_qname("Person:middleName"),
    model.get_qname("Person:lastName"),
    model.get_qname("Person:fatherName"),
    model.get_qname("Person:motherName"),
}

log = get_logger(__name__)


def clean_identifier(prop: Property, value: str) -> Optional[str]:
    normalized: Optional[str] = value
    if prop.format in VALIDATE_FORMATS:
        format_ = get_identifier_format(prop.format)
        if format_ is not None:
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


def is_lookup_value(entity: "Entity", type_: PropertyType, value: str) -> bool:
    """Check if a given value for a certain property type was obtained from
    a lookup. This is used to skip validation for looked-up values."""
    lookup = get_type_lookup(entity.dataset, type_)
    if lookup is None:
        return False
    result = lookup.match(value)
    return result is not None


def value_clean(
    entity: "Entity",
    prop: Property,
    value: Optional[str],
    cleaned: bool = False,
    fuzzy: bool = False,
    format: Optional[str] = None,
) -> Generator[Tuple[Property, str], None, None]:
    if prop.deprecated:
        log.warning(
            "Deprecated property used: %s" % prop.name,
            entity_id=entity.id,
            schema=entity.schema.name,
            prop=prop.name,
        )

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
        # We validate Person:*Name properties as names cause they're strings
        # in the FtM model.
        # See https://github.com/opensanctions/followthemoney/issues/71
        # to track creation of a NamePart type.
        if (
            prop_.type == registry.name or prop_ in VALIDATE_AS_NAME_PROPS
        ) and clean is not None:
            clean = unicodedata.normalize("NFC", clean)

            # FIXME: this is a work-around to introduce the abbreviation prop.
            # It should be ready to go out in Q3/Q4 2026. See:
            # https://github.com/opensanctions/opensanctions/issues/3297
            if prop_.name == "abbreviation" and clean is not None:
                weak_prop = entity.schema.get("weakAlias")
                if weak_prop is not None:
                    yield weak_prop, clean
                # Allow abbreviations that are not valid names

            elif entity.schema.is_a("LegalEntity") and not is_name(clean):
                if not is_lookup_value(entity, registry.name, item):
                    log.warning(
                        f"Property value {value!r} is not a valid name.",
                        entity_id=entity.id,
                        value=value,
                        clean=clean,
                        prop=prop_.name,
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
