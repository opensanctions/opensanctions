from normality import slugify
from functools import lru_cache
from typing import Optional
from addressformatting import AddressFormatter
from followthemoney.types import registry
from followthemoney.util import make_entity_id, join_text

from opensanctions.core.context import Context
from opensanctions.core.entity import Entity
from opensanctions.core.lookups import common_lookups


@lru_cache(maxsize=None)
def get_formatter() -> AddressFormatter:
    return AddressFormatter()


def clean_address(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    lookup = common_lookups().get("fulladdress")
    if lookup is None:
        raise RuntimeError("No fulladdress datapatches are configured.")
    return lookup.get_value(value, default=value)


def make_address(
    context: Context,
    full=None,
    remarks=None,
    summary=None,
    po_box=None,
    street=None,
    street2=None,
    street3=None,
    city=None,
    place=None,
    postal_code=None,
    state=None,
    region=None,
    country=None,
    country_code=None,
    key=None,
) -> Entity:
    """Generate an address schema object adjacent to the main entity."""

    city = join_text(place, city, sep=", ")
    street = join_text(street, street2, street3, sep=", ")

    address = context.make("Address")
    address.add("full", full)
    address.add("remarks", remarks)
    address.add("summary", summary)
    address.add("postOfficeBox", po_box)
    address.add("street", street)
    address.add("city", city)
    address.add("postalCode", postal_code)
    address.add("region", region)
    address.add("state", state, quiet=True)
    address.add("country", country)
    address.add("country", country_code)

    country_code = address.first("country")
    if not address.has("full"):
        data = {
            "attention": summary,
            "house": po_box,
            "road": street,
            "postcode": postal_code,
            "city": city,
            "state": join_text(region, state, sep=", "),
            # "country": country,
        }
        full = get_formatter().one_line(data, country=country_code)
        address.add("full", full)

    full_country = registry.country.clean(full)
    if full_country is not None:
        # print("FULL COUNTRY", full, full_country)
        address.add("country", full_country)
        # full = None

    full = clean_address(full)
    address.set("full", full)

    if full:
        norm_full = slugify(full)
        hash_id = make_entity_id(country_code, norm_full, key)
        if hash_id is not None:
            address.id = f"addr-{hash_id}"
    return address


def apply_address(context: Context, entity: Entity, address: Entity):
    """Link the given entity to the given address."""
    if address is None:
        return
    entity.add("country", address.get("country"))
    if address.id is not None:
        entity.add("addressEntity", address)
        context.emit(address)
