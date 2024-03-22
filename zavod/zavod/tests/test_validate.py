import pytest
from structlog.testing import capture_logs

from zavod.context import Context
from zavod.store import get_store
from zavod.validators import DanglingReferencesValidator, SelfReferenceValidator, TopiclessTargetValidator
from zavod.archive import clear_data_path
from zavod.crawl import crawl_dataset
from zavod.validators.assertions import AssertionsValidator


def test_dangling_references(testdataset3) -> None:
    clear_data_path(testdataset3.name)
    crawl_dataset(testdataset3)
    context = Context(testdataset3)
    store = get_store(testdataset3)
    view = store.view(testdataset3)

    with capture_logs() as cap_logs:
        validator = DanglingReferencesValidator(context, view)
        for entity in view.entities():
            validator.feed(entity)
        with pytest.raises(ValueError) as exc_info:
            validator.finish()
        assert str(exc_info.value) == "Dangling references found"

    store.close()
    context.close()

    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert (
        "error: td3-child-of-nonexistent-co property parent references missing id td3-nonexistent-co"
    ) in logs, logs
    assert (
        "error: td3-asset-of-nonexistent-co-ownership-nonexistent-co property owner references missing id td3-nonexistent-co"
    ) in logs, logs
    assert len(logs) == 2, logs


def test_self_references(testdataset3) -> None:
    clear_data_path(testdataset3.name)
    crawl_dataset(testdataset3)
    context = Context(testdataset3)
    store = get_store(testdataset3)
    view = store.view(testdataset3)

    with capture_logs() as cap_logs:
        validator = SelfReferenceValidator(context, view)
        for entity in view.entities():
            validator.feed(entity)
        validator.finish()
    context.close()
    store.close()

    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert (
        "warning: td3-owner-of-self-co references itself via ownershipOwner"
        " -> td3-owner-of-self-co-ownership-owner-of-self-co -> asset"
    ) in logs, logs
    assert (
        "warning: td3-owner-of-self-co references itself via ownershipAsset"
        " -> td3-owner-of-self-co-ownership-owner-of-self-co -> owner"
    ) in logs, logs
    assert len(logs) == 2, logs


def test_topicless_targets(testdataset3) -> None:
    clear_data_path(testdataset3.name)
    crawl_dataset(testdataset3)
    context = Context(testdataset3)
    store = get_store(testdataset3)
    view = store.view(testdataset3)

    with capture_logs() as cap_logs:
        validator = TopiclessTargetValidator(context, view)
        for entity in view.entities():
            validator.feed(entity)
        validator.finish()
    context.close()
    store.close()

    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert "warning: td3-target-no-topic-co is a target but has no topics" in logs, logs
    assert len(logs) == 1, logs


def test_assertions(testdataset1) -> None:
    clear_data_path(testdataset1.name)
    crawl_dataset(testdataset1)
    context = Context(testdataset1)
    store = get_store(testdataset1)
    view = store.view(testdataset1)

    with capture_logs() as cap_logs:
        validator = AssertionsValidator(context, view)
        for entity in view.entities():
            validator.feed(entity)
        validator.finish()
    context.close()
    store.close()

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
        "warning: Assertion failed for value 6: "
        "<Assertion entity_count gte 10 filter: schema=Person>"
    ) in logs, logs
    assert (
        "warning: Assertion failed for value 6: <Assertion country_count gte 7>"
    ) in logs, logs
