from json import loads

from zavod import settings
from zavod.meta import Dataset
from zavod.archive import clear_data_path
from zavod.exporters.senzing import SenzingExporter, canonical_national_id_type
from zavod.crawl import crawl_dataset
from zavod.tests.exporters.util import harnessed_export


def test_canonical_national_id_type():
    """The Identification adjacency must canonicalize its free-text FtM `type` to the SAME
    upper-case scheme the typed properties and the Sayari mapper emit, so the same scheme
    bridges across sources; generic/unrecognized labels must fall back to a blank type."""
    # Deducible + canonical -> canonical scheme (equals the Sayari mapper's labels).
    assert canonical_national_id_type("C.U.R.P.") == "CURP"
    assert canonical_national_id_type("Cedula No.") == "CEDULA"
    assert canonical_national_id_type("D.N.I.") == "DNI"
    assert canonical_national_id_type("C.U.I.T.") == "CUIT"
    assert canonical_national_id_type("C.U.I.") == "CUI"
    assert canonical_national_id_type("Commercial Register") == "REGISTRATION_NUMBER"
    # Case / punctuation variants collapse to the same canonical scheme.
    assert canonical_national_id_type("curp") == "CURP"
    assert canonical_national_id_type("CURP") == "CURP"
    assert canonical_national_id_type("dni") == "DNI"
    # C.U.I. and C.U.I.T. must NOT collide.
    assert canonical_national_id_type("C.U.I.") != canonical_national_id_type("C.U.I.T.")
    # Generic or unrecognized -> blank (None); Senzing learns the untyped id.
    assert canonical_national_id_type("National ID No.") is None
    assert canonical_national_id_type("Identification Number") is None
    assert canonical_national_id_type("N.I.E.") is None
    assert canonical_national_id_type("Some Unknown Doc Type") is None
    assert canonical_national_id_type(None) is None


def test_senzing(testdataset1: Dataset):
    """Tests whether the senzing output contain the expected entities, with expected
    keys and value formats."""
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    harnessed_export(SenzingExporter, testdataset1)

    with open(dataset_path / "senzing.json") as senzing_file:
        targets = [loads(line) for line in senzing_file.readlines()]
    # The exporter emits the single-list FEATURES schema: every feature is an object in FEATURES;
    # only DATA_SOURCE/RECORD_ID/LAST_CHANGE/URL stay at the top level.
    company = [t for t in targets if t["RECORD_ID"] == "osv-umbrella-corp"][0]
    company_features = company["FEATURES"]

    assert {"RECORD_TYPE": "ORGANIZATION"} in company_features
    assert {
        "NAME_TYPE": "PRIMARY",
        "NAME_ORG": "Umbrella Corporation",
    } in company_features
    assert {
        "NAME_TYPE": "ALIAS",
        "NAME_ORG": "Umbrella Pharmaceuticals, Inc.",
    } in company_features
    assert {"REGISTRATION_DATE": "1980"} in company_features
    assert {"REGISTRATION_COUNTRY": "us"} in company_features
    # `registrationNumber` is a distinct scheme (a company registration number), kept as
    # NATIONAL_ID_TYPE so it doesn't collide with personal national-id schemes. Only the truly
    # generic defaults (a bare `idNumber` / `taxNumber`) are left blank so cross-source ids bridge
    # instead of conflicting when the type is scored.
    assert {
        "NATIONAL_ID_NUMBER": "8723-BX",
        "NATIONAL_ID_TYPE": "REGISTRATION_NUMBER",
        "NATIONAL_ID_COUNTRY": "us",
    } in company_features
    assert company["DATA_SOURCE"] == "OS_TESTDATASET1"
    assert company["RECORD_ID"] == "osv-umbrella-corp"
    assert "/entities/osv-umbrella-corp" in company["URL"]
    assert company["LAST_CHANGE"] is not None

    person = [t for t in targets if t["RECORD_ID"] == "osv-hans-gruber"][0]
    person_features = person["FEATURES"]
    assert {"RECORD_TYPE": "PERSON"} in person_features
    assert {"NAME_TYPE": "PRIMARY", "NAME_FULL": "Hans Gruber"} in person_features
    assert {"NAME_TYPE": "ALIAS", "NAME_FULL": "Bill Clay"} in person_features
    assert {"ADDR_FULL": "Lauensteiner Str. 49, 01277 Dresden"} in person_features
    assert {"DATE_OF_BIRTH": "1978-09-25"} in person_features
    assert {"NATIONALITY": "dd"} in person_features
    assert person["DATA_SOURCE"] == "OS_TESTDATASET1"
    assert person["RECORD_ID"] == "osv-hans-gruber"
