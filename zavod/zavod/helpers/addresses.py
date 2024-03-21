from functools import cache, lru_cache
from typing import Optional
from addressformatting import AddressFormatter
from followthemoney.types import registry
from followthemoney.util import join_text, make_entity_id
from normality import slugify

from zavod.entity import Entity
from zavod.context import Context
from zavod.runtime.lookups import type_lookup


@cache
def _get_formatter() -> AddressFormatter:
    return AddressFormatter()


@lru_cache(maxsize=5000)
def format_address(
    summary: Optional[str] = None,
    po_box: Optional[str] = None,
    street: Optional[str] = None,
    house: Optional[str] = None,
    house_number: Optional[str] = None,
    postal_code: Optional[str] = None,
    city: Optional[str] = None,
    county: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    country_code: Optional[str] = None,
) -> str:
    """Given the components of a postal address, format it into a single line
    using some country-specific templating logic.

    Args:
        summary: A short description of the address.
        po_box: The PO box/mailbox number.
        street: The street or road name.
        house: The descriptive name of the house.
        house_number: The number of the house on the street.
        postal_code: The postal code or ZIP code.
        city: The city or town name.
        county: The county or district name.
        state: The state or province name.
        country: The name of the country (words, not ISO code).
        country_code: A pre-normalized country code.

    Returns:
        A single-line string with the formatted address."""
    if country_code is None and country is not None:
        country_code = registry.country.clean_text(country)
    data = {
        "attention": summary,
        "road": street,
        "house": po_box or house,
        "house_number": house_number,
        "postcode": postal_code,
        "city": city,
        "county": county,
        "state_district": state,
        "country": country,
    }
    return _get_formatter().one_line(data, country=country_code)


def _make_id(
    entity: Entity,
    full: Optional[str],
    country_code: Optional[str],
    key: Optional[str] = None,
) -> Optional[str]:
    if full is None or not len(full.strip()):
        country_id = make_entity_id(country_code, full, key)
        if country_id is None:
            return None
        return f"addr-{country_id}"
    for norm in type_lookup(entity.dataset, registry.address, full):
        norm_full = slugify(norm)
        if norm_full is None:
            continue
        hashed_id = make_entity_id(country_code, norm_full, key)
        if hashed_id is not None:
            return f"addr-{hashed_id}"
    return None


def make_address(
    context: Context,
    full: Optional[str] = None,
    remarks: Optional[str] = None,
    summary: Optional[str] = None,
    po_box: Optional[str] = None,
    street: Optional[str] = None,
    street2: Optional[str] = None,
    street3: Optional[str] = None,
    city: Optional[str] = None,
    place: Optional[str] = None,
    postal_code: Optional[str] = None,
    state: Optional[str] = None,
    region: Optional[str] = None,
    country: Optional[str] = None,
    country_code: Optional[str] = None,
    key: Optional[str] = None,
    lang: Optional[str] = None,
) -> Optional[Entity]:
    """Generate an address schema object adjacent to the main entity.

    Args:
        context: The runner context used for making and emitting entities.
        full: The full address as a single string.
        remarks: Delivery remarks for the address.
        summary: A short description of the address.
        po_box: The PO box/mailbox number.
        street: The street or road name.
        street2: The street or road name, line 2.
        street3: The street or road name, line 3.
        city: The city or town name.
        place: The name of a smaller locality (same as city).
        postal_code: The postal code or ZIP code.
        state: The state or province name.
        region: The region or district name.
        country: The country name (words, not ISO code).
        country_code: A pre-normalized country code.
        key: An optional key to be included in the ID of the address.
        lang: The language of the address details.

    Returns:
        A new entity of type `Address`."""
    city = join_text(place, city, sep=", ")
    street = join_text(street, street2, street3, sep=", ")

    # This is meant to handle cases where the country field contains a country code
    # in a subset of the given records:
    if country is not None and len(country.strip()) == 2:
        context.log.warn(
            "Country name looks like a country code",
            country=country,
            country_code=country_code,
        )
        if country_code is None:
            country_code = country
            country = None

    if country is not None:
        parsed_code = registry.country.clean(country)
        if parsed_code is not None:
            if country_code is not None and country_code != parsed_code:
                context.log.warn(
                    "Country code mismatch",
                    country=country,
                    country_code=country_code,
                )
            country_code = parsed_code

    if country_code is None:
        country_code = registry.country.clean(full)

    if not full:
        full = format_address(
            summary=summary,
            po_box=po_box,
            street=street,
            postal_code=postal_code,
            city=city,
            state=join_text(region, state, sep=", "),
            country=country,
            country_code=country_code,
        )

    if full == country:
        full = None

    address = context.make("Address")
    address.id = _make_id(address, full, country_code, key=key)
    if address.id is None:
        return None

    address.add("full", full, lang=lang)
    address.add("remarks", remarks, lang=lang)
    address.add("summary", summary, lang=lang)
    address.add("postOfficeBox", po_box, lang=lang)
    address.add("street", street, lang=lang)
    address.add("city", city, lang=lang)
    address.add("postalCode", postal_code, lang=lang)
    address.add("region", region, lang=lang)
    address.add("state", state, quiet=True, lang=lang)
    address.add("country", country_code, lang=lang, original_value=country)
    return address


def apply_address(context: Context, entity: Entity, address: Optional[Entity]) -> None:
    """Link the given entity to the given address and emits the address.

    Args:
        context: The runner context used for emitting entities.
        entity: The thing located at the given address.
        address: The address entity, usually constructed with `make_address`.
    """
    if address is None:
        return
    assert address.schema.is_a("Address"), "address must be an Address"
    assert (
        entity.schema.get("addressEntity") is not None
    ), "Entity must have addressEntity"
    entity.add("country", address.get("country"))
    if address.has("full"):
        entity.add("addressEntity", address)
        context.emit(address)


def copy_address(entity: Entity, address: Optional[Entity]) -> None:
    """Assign to full address text and country directly to the given entity.

    This is an alternative to using `apply_address` when the address should
    be inlined into the entity, instead of emitting a separate address object.

    Args:
        entity: The entity to be assigned the address.
        address: The address entity to be copied into the entity.
    """
    if address is not None:
        entity.add("address", address.get("full"))
        for country in address.get("country"):
            if country not in entity.countries:
                entity.add("country", country)
