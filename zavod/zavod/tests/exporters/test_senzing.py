from json import loads

from zavod import settings
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.archive import clear_data_path, dataset_artifact_directory
from zavod.exporters.senzing import (
    canonical_id,
    SenzingExporter,
    classify_identification,
    crypto_id_type,
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
    # DATA_SOURCE/RECORD_ID/LAST_CHANGE/URL and the non-feature payload stay at the top level.
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
    # A dataset-scoped source id is not an identity: no OpenSanctions id features at all.
    assert not [f for f in person_features if "OPENSANCTIONS_ID" in str(f)]
    assert "OPENSANCTIONS_ID" not in person
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


def test_crypto_id_type():
    """A wallet's asset becomes the crosswalk code, whichever name the source used."""

    def wallet(**props):
        return Entity(
            Dataset({"name": "test", "title": "Test"}),
            {"schema": "CryptoWallet", "id": "w", "properties": props},
        )

    assert crypto_id_type(wallet(currency=["bitcoin"])) == "CRYPTO_XBT"
    assert crypto_id_type(wallet(currency=["BTC"])) == "CRYPTO_XBT"
    assert crypto_id_type(wallet(currency=["Ethereum"])) == "CRYPTO_ETH"
    assert crypto_id_type(wallet(currency=["USDT"])) == "CRYPTO_USDT"
    assert crypto_id_type(wallet()) == "CRYPTO"


def test_wallet_on_holder(tmp_path):
    """A held wallet's address is an identifier of its holder; an unheld wallet stays a record."""
    ds = Dataset({"name": "test", "title": "Test"})
    holder = Entity(
        ds, {"schema": "Company", "id": "h", "properties": {"name": ["Exchange"]}}
    )
    held = Entity(
        ds,
        {
            "schema": "CryptoWallet",
            "id": "w1",
            "properties": {"holder": ["h"], "publicKey": ["1Abc"], "currency": ["BTC"]},
        },
    )
    unheld = Entity(
        ds,
        {"schema": "CryptoWallet", "id": "w2", "properties": {"publicKey": ["1Xyz"]}},
    )

    class View:
        def get_adjacent(self, entity):
            if entity.id == "h":
                yield held.schema.get("holder"), held

    exporter = SenzingExporter.__new__(SenzingExporter)
    exporter.dataset = ds
    exporter.path = tmp_path / "senzing.json"
    exporter.setup()
    for entity in (holder, held, unheld):
        exporter.feed(entity, View())
    exporter.fh.close()
    records = {r["RECORD_ID"]: r for r in map(loads, open(exporter.path))}
    assert set(records) == {"h", "w2"}
    wallet = {"OTHER_ID_NUMBER": "1Abc", "OTHER_ID_TYPE": "CRYPTO_XBT"}
    assert wallet in records["h"]["FEATURES"]
    assert {"OTHER_ID_NUMBER": "1Xyz", "OTHER_ID_TYPE": "CRYPTO"} in records["w2"][
        "FEATURES"
    ]


def test_features_shape(tmp_path):
    """FEATURES holds only Senzing feature attributes and never an empty object; descriptive
    values that are not features (topics, source URLs, dissolution date) are root payload
    strings, never lists or objects."""
    ds = Dataset({"name": "test", "title": "Test"})
    company = Entity(
        ds,
        {
            "schema": "Company",
            "id": "c",
            "properties": {
                "name": ["Acme"],
                "topics": ["sanction", "debarment"],
                "sourceUrl": ["https://example.org/a"],
                "dissolutionDate": ["2020-01-02"],
                "wikidataId": ["Q42"],
            },
        },
    )
    no_number = Entity(
        ds,
        {
            "schema": "Identification",
            "id": "i",
            "properties": {"holder": ["c"], "country": ["ru"]},
        },
    )
    no_text = Entity(ds, {"schema": "Address", "id": "a", "properties": {}})

    class View:
        def get_adjacent(self, entity):
            yield no_number.schema.get("holder"), no_number
            yield company.schema.get("addressEntity"), no_text

    exporter = SenzingExporter.__new__(SenzingExporter)
    exporter.dataset = ds
    exporter.path = tmp_path / "senzing.json"
    exporter.setup()
    exporter.feed(company, View())
    exporter.fh.close()
    (record,) = map(loads, open(exporter.path))
    feats = record["FEATURES"]
    assert not [f for f in feats if "OPENSANCTIONS_ID" in str(f)]
    # A cited Wikidata QID is the plain WIKIDATA_ID feature, never a TRUSTED_ID.
    assert {"WIKIDATA_ID": "Q42"} in feats
    assert not [f for f in feats if "TRUSTED_ID_TYPE" in f]
    assert all(feats), "an empty FEATURES object"
    assert not [f for f in feats if "NATIONAL_ID_COUNTRY" in f]
    for attr in ("TOPIC", "SOURCE_URL", "DISSOLUTION_DATE"):
        assert not [f for f in feats if attr in f]
    assert "OPENSANCTIONS_ID" not in record
    assert sorted(record["TOPIC"].split("; ")) == ["debarment", "sanction"]
    assert record["SOURCE_URL"] == "https://example.org/a"
    assert record["DISSOLUTION_DATE"] == "2020-01-02"
    assert not [
        k for k, v in record.items() if k != "FEATURES" and isinstance(v, (list, dict))
    ]


def test_figi_many_per_security(tmp_path):
    """A security holds one FIGI per listing: each becomes its own OTHER_ID typed FIGI."""
    ds = Dataset({"name": "test", "title": "Test"})
    security = Entity(
        ds,
        {
            "schema": "Security",
            "id": "s",
            "properties": {
                "name": ["Example Shares"],
                "figiCode": ["BBG000000001", "BBG000000002"],
            },
        },
    )

    class View:
        def get_adjacent(self, entity):
            return iter(())

    exporter = SenzingExporter.__new__(SenzingExporter)
    exporter.dataset = ds
    exporter.path = tmp_path / "senzing.json"
    exporter.setup()
    exporter.feed(security, View())
    exporter.fh.close()
    (record,) = map(loads, open(exporter.path))
    figis = [f for f in record["FEATURES"] if f.get("OTHER_ID_TYPE") == "FIGI"]
    assert sorted(f["OTHER_ID_NUMBER"] for f in figis) == [
        "BBG000000001",
        "BBG000000002",
    ]
    assert not [f for f in record["FEATURES"] if f.get("OTHER_ID_TYPE") == "figiCode"]


def test_opensanctions_id_only_when_canonical():
    # NK- (deduplicated) and Wikidata QIDs are identities; source ids and look-alikes are not.
    for entity_id in ("NK-2tuTdT4xoDNVi9SnYbdLMu", "Q42"):
        assert canonical_id(entity_id), entity_id
    for entity_id in (
        "br-pep-66e5",
        "ru-inn-7704028206",
        "Q0",
        "Q42-x",
        "nk-abc",
        "NK-",
    ):
        assert not canonical_id(entity_id), entity_id


def export_one(tmp_path, data):
    """Export a single entity (no adjacent entities) and return its Senzing record."""
    ds = Dataset({"name": "test", "title": "Test"})

    class View:
        def get_adjacent(self, entity):
            return iter(())

    exporter = SenzingExporter.__new__(SenzingExporter)
    exporter.dataset = ds
    exporter.path = tmp_path / "senzing.json"
    exporter.setup()
    exporter.feed(Entity(ds, data), View())
    exporter.fh.close()
    (record,) = map(loads, open(exporter.path))
    return record


def test_retired_qid_is_not_an_identity(tmp_path):
    """A QID entity whose wikidataId names another item carries a retired (redirected) QID."""
    retired = export_one(
        tmp_path,
        {
            "schema": "Person",
            "id": "Q16119148",
            "properties": {"name": ["Jane Example"], "wikidataId": ["Q12223619"]},
        },
    )
    feats = retired["FEATURES"]
    assert not [
        f for f in feats if "REF_OPENSANCTIONS_ID" in f or "OPENSANCTIONS_ID" in f
    ]
    # Both QIDs still emitted, so it links to the entity holding the surviving QID.
    assert sorted(f["WIKIDATA_ID"] for f in feats if "WIKIDATA_ID" in f) == [
        "Q12223619",
        "Q16119148",
    ]
    surviving = export_one(
        tmp_path,
        {
            "schema": "Person",
            "id": "Q12223619",
            "properties": {"name": ["Jane Example"], "wikidataId": ["Q12223619"]},
        },
    )
    assert {"REF_OPENSANCTIONS_ID": "Q12223619"} in surviving["FEATURES"]
    assert {"OPENSANCTIONS_ID": "Q12223619"} in surviving["FEATURES"]


def test_inn_is_always_russian(tmp_path):
    """INN is a Russian scheme: its country is ru even when the entity is registered elsewhere."""
    record = export_one(
        tmp_path,
        {
            "schema": "Company",
            "id": "NK-abc",
            "properties": {
                "name": ["Example Ltd"],
                "jurisdiction": ["gb"],
                "innCode": ["1327025929"],
            },
        },
    )
    (inn,) = [f for f in record["FEATURES"] if f.get("TAX_ID_TYPE") == "INN"]
    assert inn["TAX_ID_COUNTRY"] == "ru"
