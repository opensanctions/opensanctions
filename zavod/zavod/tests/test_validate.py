from typing import Type

from structlog.testing import capture_logs

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

    cap_logs = [(log["log_level"], log["event"]) for log in cap_logs]
    return validator, set(cap_logs)


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
    validator, logs = run_validator(AssertionsValidator, testdataset3)
    assert (
        "warning",
        "Assertion failed for value 2: <Assertion entity_count gte 3 filter: country=de>",
    ) in logs
    assert (
        "warning",
        "Assertion failed for value 2: <Assertion entity_count lte 1 filter: country=de>",
    ) in logs
    assert (
        "warning",
        "Assertion failed for value 7: <Assertion entity_count gte 10 filter: schema=Company>",
    ) in logs
    assert (
        "warning",
        "Assertion failed for value 6: <Assertion country_count gte 7>",
    ) in logs
    assert (
        "warning",
        "Assertion failed for value 7: <Assertion entities_with_prop_count gte 11 filter: entities_with_prop=('Company', 'name')>",
    ) in logs
    assert ("error", "One or more assertions failed.") in logs
    assert validator.abort is True


def test_no_assertions_warning(testdataset3: Dataset) -> None:
    testdataset3.assertions = []
    crawl_dataset(testdataset3)
    validator, logs = run_validator(AssertionsValidator, testdataset3)
    assert ("warning", "Dataset has no assertions.") in logs


def test_not_empty_after_crawl(testdataset3) -> None:
    crawl_dataset(testdataset3)
    validator, logs = run_validator(EmptyValidator, testdataset3)
    assert "No entities validated" not in str(logs)
    assert validator.abort is False


def test_no_crawl_is_empty(testdataset3) -> None:
    # We don't crawl here so that no entities are emitted
    validator, logs = run_validator(EmptyValidator, testdataset3)
    assert ("warning", "No entities validated.") in logs
    assert validator.abort is False
