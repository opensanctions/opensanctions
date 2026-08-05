from normality import slugify
from rigour.ids import IMO


def _imo_id_key(value: str | None) -> str | None:
    """Derive the IMO portion of an entity id from a raw IMO string.

    A valid IMO is reduced to its canonical seven digits. A present-but-invalid one (bad
    checksum, wrong length, stray text) falls back to a slug of the raw value, so a faulty
    source IMO still yields a stable key rather than being discarded. Returns None only when
    there is no usable text at all.
    """
    if value is None:
        return None
    normalized = IMO.normalize(value)
    if normalized is not None:
        return normalized.removeprefix("IMO")
    return slugify(value)


def make_vessel_imo_id(value: str | None) -> str | None:
    """Build a stable entity id for a vessel from its IMO number.

    Reach for this when keying vessels so that records describing the same ship across
    sources converge on one entity without depending on any source's internal numbering.
    Valid IMOs collapse to their seven digits; malformed ones fall back to a slug of the raw
    value so a faulty IMO keeps the vessel rather than dropping it. Returns None when no IMO
    text is supplied — the caller should then key the entity another way.
    """
    key = _imo_id_key(value)
    return None if key is None else f"imo-vsl-{key}"


def make_org_imo_id(value: str | None) -> str | None:
    """Build a stable entity id for an organisation from its IMO company number.

    The maritime equivalent of a company register id: registered owners, managers and other
    shipping companies carry an IMO company number. Use this to key those organisations the
    same way [make_vessel_imo_id][zavod.helpers.make_vessel_imo_id] keys ships. Returns None
    when no IMO text is supplied.
    """
    key = _imo_id_key(value)
    return None if key is None else f"imo-org-{key}"
