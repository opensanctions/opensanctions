from json import loads

from zavod import settings
from zavod.meta import Dataset
from zavod.archive import clear_data_path, dataset_artifact_directory
from zavod.exporters.senzing import (
    SenzingExporter,
    classify_identification,
    ogrn_subtype,
)
from zavod.crawl import crawl_dataset
from zavod.tests.exporters.util import harnessed_export


def test_classify_identification():
    """An Identification's free-text `type` decides both the Senzing feature and the canonical
    scheme within it, so the same scheme bridges across sources; generic or unrecognized
    labels fall back to an untyped NATIONAL_ID."""
    # National-id schemes -> canonical scheme (+ the issuing country when there is only one).
    assert classify_identification("C.U.R.P.") == ("NATIONAL_ID", "CURP", "mx")
    assert classify_identification("Cedula No.") == ("NATIONAL_ID", "CEDULA", None)
    assert classify_identification("D.N.I.") == ("NATIONAL_ID", "DNI", None)
    assert classify_identification("C.U.I.") == ("NATIONAL_ID", "CUI", None)
    assert classify_identification("N.I.E.") == ("NATIONAL_ID", "NIE", "es")
    assert classify_identification("Commercial Register") == (
        "NATIONAL_ID",
        "REGISTRATION_NUMBER",
        None,
    )
    # Case / punctuation variants collapse to the same scheme.
    assert classify_identification("curp") == ("NATIONAL_ID", "CURP", "mx")
    assert classify_identification("dni") == ("NATIONAL_ID", "DNI", None)
    # CPF / CNPJ are Brazilian tax ids; a CUIL stays a national id.
    assert classify_identification("CPF") == ("TAX_ID", "CPF", "br")
    assert classify_identification("CNPJ") == ("TAX_ID", "CNPJ", "br")
    assert classify_identification("C.U.I.L.") == ("NATIONAL_ID", "CUIL", "ar")
    # A CUIT is a tax id, not a national id; C.U.I. and C.U.I.T. must not collide.
    assert classify_identification("C.U.I.T.") == ("TAX_ID", "CUIT", "ar")
    # Other documents get their own feature or OTHER_ID scheme.
    assert classify_identification("Driver's License No.") == ("DRLIC", None, None)
    assert classify_identification("License") == ("OTHER_ID", "LICENSE", None)
    assert classify_identification("Birth Certificate Number") == (
        "OTHER_ID",
        "BIRTH_CERT",
        None,
    )
    # Unlisted labels are typed by keyword.
    assert classify_identification("Driving Licence") == ("DRLIC", None, None)
    assert classify_identification("Service Passport") == ("PASSPORT", None, None)
    assert classify_identification("Business Licence") == ("OTHER_ID", "LICENSE", None)
    assert classify_identification("Tax Registration No.") == ("TAX_ID", None, None)
    # Not an identifier at all.
    assert classify_identification("Government Gazette Number")[0] is None
    # Generic or unrecognized -> untyped NATIONAL_ID; Senzing learns the untyped id.
    assert classify_identification("National ID No.") == ("NATIONAL_ID", None, None)
    assert classify_identification("Identification Number") == (
        "NATIONAL_ID",
        None,
        None,
    )
    assert classify_identification("Some Unknown Doc Type") == (
        "NATIONAL_ID",
        None,
        None,
    )
    assert classify_identification(None) == ("NATIONAL_ID", None, None)


def test_ogrn_subtype():
    assert ogrn_subtype("1027700132195") == "OGRN"  # 13 digits: a legal entity
    assert ogrn_subtype("315183100004295") == "OGRNIP"  # 15: individual entrepreneur


def test_senzing(testdataset1: Dataset):
    """Tests whether the senzing output contain the expected entities, with expected
    keys and value formats."""
    dataset_path = dataset_artifact_directory(testdataset1.name, settings.RUN_VERSION)
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1, settings.RUN_VERSION)
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
    # The OpenSanctions id is asserted once, as the OPENSANCTIONS_ID feature, not again as an
    # OTHER_ID.
    assert person["OPENSANCTIONS_ID"] == "osv-hans-gruber"
    assert not [
        f for f in person_features if f.get("OTHER_ID_TYPE") == "OPEN_SANCTIONS"
    ]


def test_source_name(tmp_path):
    """DATA_SOURCE: a source dataset is OS_<NAME> unless overridden; a collection is
    OPEN_SANCTIONS, except the openownership collection, which keeps its override."""

    def source_name(data):
        exporter = SenzingExporter.__new__(SenzingExporter)
        exporter.dataset = Dataset({"title": "Test", **data})
        exporter.path = tmp_path / "senzing.json"
        exporter.setup()
        exporter.fh.close()
        return exporter.source_name

    assert source_name({"name": "us_ofac_sdn"}) == "OS_US_OFAC_SDN"
    assert source_name({"name": "gleif"}) == "GLEIF"
    assert source_name({"name": "default", "children": ["gleif"]}) == "OPEN_SANCTIONS"
    assert (
        source_name({"name": "openownership", "children": ["gleif"]})
        == "OPEN_OWNERSHIP"
    )
