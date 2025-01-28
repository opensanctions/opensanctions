from typing import Type

from structlog.testing import capture_logs

from zavod.archive import clear_data_path
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
from zavod.validators.assertions import AssertionsValidator
from zavod.validators.common import BaseValidator


def run_validator(clazz: Type[BaseValidator], dataset: Dataset):
    context = Context(dataset)
    linker = get_dataset_linker(dataset)
    store = get_store(dataset, linker)
    store.sync()
    view = store.view(dataset)

    with capture_logs() as cap_logs:
        validator = clazz(context, view)
        for entity in view.entities():
            validator.feed(entity)
        validator.finish()

    store.close()
    context.close()
    return validator, cap_logs


def test_dangling_references(testdataset3) -> None:
    clear_data_path(testdataset3.name)
    crawl_dataset(testdataset3)
    validator, cap_logs = run_validator(DanglingReferencesValidator, testdataset3)

    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert (
        "warning: td3-child-of-nonexistent-co property parent references missing id td3-nonexistent-co"
    ) in logs, logs
    assert (
        "warning: td3-asset-of-nonexistent-co-ownership-nonexistent-co property owner references missing id td3-nonexistent-co"
    ) in logs, logs
    assert len(logs) == 2, logs
    assert validator.abort is False


def test_self_references(testdataset3) -> None:
    clear_data_path(testdataset3.name)
    crawl_dataset(testdataset3)
    validator, cap_logs = run_validator(SelfReferenceValidator, testdataset3)

    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert (
        "info: td3-owner-of-self-co references itself via ownershipOwner"
        " -> td3-owner-of-self-co-ownership-owner-of-self-co -> asset"
    ) in logs, logs
    assert (
        "info: td3-owner-of-self-co references itself via ownershipAsset"
        " -> td3-owner-of-self-co-ownership-owner-of-self-co -> owner"
    ) in logs, logs
    assert len(logs) == 2, logs
    assert validator.abort is False


def test_assertions(testdataset3) -> None:
    clear_data_path(testdataset3.name)
    crawl_dataset(testdataset3)
    validator, cap_logs = run_validator(AssertionsValidator, testdataset3)

    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert (
        "warning: Assertion failed for value 2: "
        "<Assertion entity_count gte 3 filter: country=de>"
    ) in logs, logs
    assert (
        "warning: Assertion failed for value 2: "
        "<Assertion entity_count lte 1 filter: country=de>"
    ) in logs, logs
    assert (
        "warning: Assertion failed for value 7: "
        "<Assertion entity_count gte 10 filter: schema=Company>"
    ) in logs, logs
    assert (
        "warning: Assertion failed for value 6: <Assertion country_count gte 7>" in logs
    ), logs
    assert (
        "warning: Assertion failed for value 7: <Assertion property_values_count gte 11 filter: property_values=('Company', 'name')>"
        in logs
    ), logs
    assert "error: One or more assertions failed." in logs, logs
    assert validator.abort is True


def test_no_assertions_warning(testdataset3: Dataset) -> None:
    testdataset3.assertions = []
    crawl_dataset(testdataset3)
    validator, cap_logs = run_validator(AssertionsValidator, testdataset3)
    assert {"log_level": "warning", "event": "Dataset has no assertions."} in cap_logs


def test_empty(testdataset3) -> None:
    clear_data_path(testdataset3.name)
    crawl_dataset(testdataset3)
    validator, cap_logs = run_validator(EmptyValidator, testdataset3)
    assert "No entities validated" not in str(cap_logs)
    assert validator.abort is False

    clear_data_path(testdataset3.name)
    validator, cap_logs = run_validator(EmptyValidator, testdataset3)
    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert "warning: No entities validated." in logs, logs
    assert validator.abort is False
