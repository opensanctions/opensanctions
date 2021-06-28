from functools import lru_cache
from addressformatting import AddressFormatter

from opensanctions.util import jointext


@lru_cache(maxsize=None)
def get_formatter():
    return AddressFormatter()


def make_address(
    context,
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
    key=None,
):
    """Generate an address schema object adjacent to the main entity."""

    city = jointext(place, city, sep=", ")
    region = jointext(region, state, sep=", ")
    street = jointext(street, street2, street3, sep=", ")

    address = context.make("Address")
    address.add("full", full)
    address.add("remarks", remarks)
    address.add("summary", summary)
    address.add("postOfficeBox", po_box)
    address.add("street", street)
    address.add("city", city)
    address.add("postalCode", postal_code)
    address.add("region", region)
    address.add("country", country)

    if not address.has("full"):
        data = {
            "attention": summary,
            "house": po_box,
            "road": street,
            "postcode": postal_code,
            "city": city,
            "state": region,
            "country": country,
        }
        cc = address.first("country")
        full = get_formatter().one_line(data, country=cc)
        address.add("full", full)

    if not address.has("full"):
        return

    address.make_id("Address", full, key)
    return address
