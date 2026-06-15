from zavod.helpers.vessels import make_vessel_imo_id, make_org_imo_id


def test_make_vessel_imo_id_valid():
    assert make_vessel_imo_id("9289518") == "imo-vsl-9289518"
    # Stray text / prefix around a valid IMO still normalizes to the digits.
    assert make_vessel_imo_id("IMO 9289518") == "imo-vsl-9289518"
    # Leading zeros are preserved (and restored) for the canonical seven digits.
    assert make_vessel_imo_id("0090524") == "imo-vsl-0090524"


def test_make_vessel_imo_id_invalid_is_kept():
    # A malformed IMO (too short / bad checksum) must not drop the entity: it falls back
    # to a slug of the raw value rather than returning None.
    assert make_vessel_imo_id("928951") == "imo-vsl-928951"
    assert make_vessel_imo_id("Unknown") == "imo-vsl-unknown"


def test_make_vessel_imo_id_empty():
    assert make_vessel_imo_id(None) is None
    assert make_vessel_imo_id("") is None
    assert make_vessel_imo_id("   ") is None


def test_make_org_imo_id():
    assert make_org_imo_id("0381931") == "imo-org-0381931"
    assert make_org_imo_id("928951") == "imo-org-928951"
    assert make_org_imo_id(None) is None
