from json import loads

from zavod import settings
from zavod.meta import Dataset
from zavod.archive import clear_data_path
from zavod.exporters.senzing import SenzingExporter
from zavod.crawl import crawl_dataset
from zavod.tests.exporters.util import harnessed_export


def test_senzing(testdataset1: Dataset):
    """Tests whether the senzing output contain the expected entities, with expected
    keys and value formats."""
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    harnessed_export(SenzingExporter, testdataset1)

    with open(dataset_path / "senzing.json") as senzing_file:
        targets = [loads(line) for line in senzing_file.readlines()]
    company = [t for t in targets if t["RECORD_ID"] == "osv-umbrella-corp"][0]
    company_names = company.pop("NAMES")

    assert {
        "NAME_TYPE": "PRIMARY",
        "NAME_ORG": "Umbrella Corporation",
    } in company_names
    assert {
        "NAME_TYPE": "ALIAS",
        "NAME_ORG": "Umbrella Pharmaceuticals, Inc.",
    } in company_names
    company_dates = company.pop("DATES")
    assert {"REGISTRATION_DATE": "1980"} in company_dates
    company_countries = company.pop("COUNTRIES")
    assert {"REGISTRATION_COUNTRY": "us"} in company_countries
    company_identifiers = company.pop("IDENTIFIERS")
    assert {"NATIONAL_ID_NUMBER": "8723-BX"} in company_identifiers
    assert company["DATA_SOURCE"] == "OS_TESTDATASET1"
    assert company["RECORD_ID"] == "osv-umbrella-corp"
    assert company["RECORD_TYPE"] == "ORGANIZATION"
    assert '/entities/osv-umbrella-corp' in company["URL"]
    assert company["LAST_CHANGE"] is not None

    person = [t for t in targets if t["RECORD_ID"] == "osv-hans-gruber"][0]
    person_names = person.pop("NAMES")
    assert {"NAME_TYPE": "PRIMARY", "NAME_FULL": "Hans Gruber"} in person_names
    assert {"NAME_TYPE": "ALIAS", "NAME_FULL": "Bill Clay"} in person_names
    person_addrs = person.pop("ADDRESSES")
    assert {"ADDR_FULL": "Lauensteiner Str. 49, 01277 Dresden"} in person_addrs
    person_dates = person.pop("DATES")
    assert {"DATE_OF_BIRTH": "1978-09-25"} in person_dates
    person_countries = person.pop("COUNTRIES")
    assert {"NATIONALITY": "dd"} in person_countries
    assert person["DATA_SOURCE"] == "OS_TESTDATASET1"
    assert person["RECORD_ID"] == "osv-hans-gruber"
    assert person["RECORD_TYPE"] == "PERSON"
