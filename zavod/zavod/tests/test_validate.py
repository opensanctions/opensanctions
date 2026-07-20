from typing import Type
import uuid

from structlog.testing import capture_logs

from zavod import Entity
from zavod.context import Context
from zavod.crawl import crawl_dataset
from zavod.integration import get_dataset_linker
from zavod.meta.dataset import Dataset
from zavod.store import get_store
from zavod.validators import (
    DanglingReferencesValidator,
    SelfReferenceValidator,
    EmptyValidator,
)
from zavod.validators.assertions import (
    StatisticsAssertionsValidator,
)
from zavod.validators.common import BaseValidator

BASE_DATASET_CONFIG = {
    "name": "test",
}


def run_validator(clazz: Type[BaseValidator], dataset: Dataset):
    context = Context(dataset)
    linker = get_dataset_linker(dataset)
    store = get_store(dataset, linker)
    # Pass clear so that if the test emits statements and re-validates, we pick that up.
    store.sync(clear=True)
    view = store.view(dataset)

    with capture_logs() as cap_logs:
        validator = clazz(context, view)
        for entity in view.entities():
            validator.feed(entity)
        validator.finish()

    store.close()
    context.close()

    cap_logs = [(log["log_level"], log["event"]) for log in cap_logs]
    return validator, set(cap_logs)


def emit_entity(ds: Dataset, schema: str, properties: dict[str, list[str]]) -> Entity:
    context = Context(ds)
    context.begin()

    entity = Entity.from_data(
        context.dataset,
        {"schema": schema, "id": uuid.uuid4(), "properties": properties},
    )
    context.emit(entity)

    context.close()
    return entity


def test_dangling_references(testdataset3) -> None:
    crawl_dataset(testdataset3)
    validator, logs = run_validator(DanglingReferencesValidator, testdataset3)

    assert logs == {
        (
            "warning",
            "td3-child-of-nonexistent-co property parent references missing id td3-nonexistent-co",
        ),
        (
            "warning",
            "td3-asset-of-nonexistent-co-ownership-nonexistent-co property owner references missing id td3-nonexistent-co",
        ),
    }
    assert validator.abort is False


def test_self_references(testdataset3) -> None:
    crawl_dataset(testdataset3)
    validator, logs = run_validator(SelfReferenceValidator, testdataset3)

    assert logs == {
        (
            "info",
            "td3-owner-of-self-co references itself via ownershipOwner -> td3-owner-of-self-co-ownership-owner-of-self-co -> asset",
        ),
        (
            "info",
            "td3-owner-of-self-co references itself via ownershipAsset -> td3-owner-of-self-co-ownership-owner-of-self-co -> owner",
        ),
    }
    assert validator.abort is False


def test_assertions(testdataset3) -> None:
    crawl_dataset(testdataset3)
    validator, logs = run_validator(StatisticsAssertionsValidator, testdataset3)
    assert (
        "error",
        "Assertion country_entities failed for de: 2 is not >= threshold 3",
    ) in logs
    assert (
        "warning",
        "Assertion country_entities failed for de: 2 is not <= threshold 1",
    ) in logs
    assert (
        "error",
        "Assertion schema_entities failed for Company: 7 is not >= threshold 10",
    ) in logs
    assert (
        "error",
        "Assertion countries failed: 6 is not >= threshold 7",
    ) in logs
    assert (
        "error",
        "Assertion entities_with_prop failed for Company.name: 7 is not >= threshold 11",
    ) in logs
    assert validator.abort is True


def test_countries_count_assertion(testdataset3) -> None:
    ds = Dataset(
        {
            **BASE_DATASET_CONFIG,
            "assertions": {
                "min": {
                    "countries": 1,
                }
            },
        }
    )
    validator, _ = run_validator(StatisticsAssertionsValidator, ds)
    assert validator.abort is True

    emit_entity(ds, "Person", {"name": ["Vladimir Putin"], "country": ["ru"]})

    validator, _ = run_validator(StatisticsAssertionsValidator, ds)
    assert validator.abort is False


def test_no_assertions_error() -> None:
    # The nudge is based on the YAML config, not the merged-in defaults: a source
    # dataset with no explicit assertions still gets nudged.
    ds = Dataset({**BASE_DATASET_CONFIG, "assertions": {}})
    assert ds.is_collection is False
    validator, logs = run_validator(StatisticsAssertionsValidator, ds)
    assert ("error", "Dataset has no assertions.") in logs


def test_default_assertions_applied_to_sources() -> None:
    # A source dataset with no explicit assertions still gets the baseline
    # defaults merged in, so validation runs and doesn't spuriously abort.
    ds = Dataset({**BASE_DATASET_CONFIG, "assertions": {}})
    assert ds.is_collection is False
    assert len(ds.assertions) > 0
    validator, _ = run_validator(StatisticsAssertionsValidator, ds)
    assert validator.abort is False


def test_default_property_fill_rate_skips_absent_schema() -> None:
    # The default Person/Organization/Company name fill-rate assertions must not
    # fail a dataset that simply doesn't emit some of those schemata.
    ds = Dataset(BASE_DATASET_CONFIG)
    emit_entity(ds, "Person", {"name": ["Vladimir Putin"]})
    validator, logs = run_validator(StatisticsAssertionsValidator, ds)
    assert validator.abort is False

    # A Person present but without a name should still fail the default.
    ds2 = Dataset({**BASE_DATASET_CONFIG, "name": "test2"})
    emit_entity(ds2, "Person", {"country": ["ru"]})
    validator, logs = run_validator(StatisticsAssertionsValidator, ds2)
    assert (
        "error",
        "Assertion property_fill_rate failed for Person.name: 0.0 is not >= threshold 0.95",
    ) in logs
    assert validator.abort is True


def test_default_property_fill_rate_company() -> None:
    # Same as the Person case, for Company: this keeps the default assertion on
    # Company covered so it isn't silently dropped.
    ds = Dataset(BASE_DATASET_CONFIG)
    emit_entity(ds, "Company", {"name": ["Acme Inc"]})
    validator, _ = run_validator(StatisticsAssertionsValidator, ds)
    assert validator.abort is False

    # A Company present but without a name should fail the default.
    ds2 = Dataset({**BASE_DATASET_CONFIG, "name": "test2"})
    emit_entity(ds2, "Company", {"country": ["ru"]})
    validator, logs = run_validator(StatisticsAssertionsValidator, ds2)
    assert (
        "error",
        "Assertion property_fill_rate failed for Company.name: 0.0 is not >= threshold 0.95",
    ) in logs
    assert validator.abort is True


def test_no_entities_warning() -> None:
    ds = Dataset(BASE_DATASET_CONFIG)

    validator, logs = run_validator(EmptyValidator, ds)
    assert "No entities validated" in str(logs)
    assert validator.abort is False

    emit_entity(ds, "Person", {"name": ["Vladimir Putin"]})
    validator, logs = run_validator(EmptyValidator, ds)
    assert "No entities validated" not in str(logs)
    assert validator.abort is False


def test_validate_assertion_property_fill_rate():
    ds = Dataset(
        {
            **BASE_DATASET_CONFIG,
            "assertions": {
                "min": {
                    "property_fill_rate": {
                        "Company": {"name": 0.5},
                    }
                }
            },
        }
    )
    emit_entity(ds, "Company", {"country": ["ru"]})

    validator, logs = run_validator(StatisticsAssertionsValidator, ds)
    assert (
        "error",
        "Assertion property_fill_rate failed for Company.name: 0.0 is not >= threshold 0.5",
    ) in logs
    assert validator.abort is True

    emit_entity(ds, "Company", {"name": ["Kalashnikov"]})
    validator, logs = run_validator(StatisticsAssertionsValidator, ds)
    assert validator.abort is False
