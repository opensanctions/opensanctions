from structlog.testing import capture_logs

from zavod.context import Context
from zavod.store import get_store
from zavod.verify import check_dangling_references, check_self_references
from zavod.archive import clear_data_path
from zavod.crawl import crawl_dataset


def test_dangling_references(testdataset3) -> None:
    clear_data_path(testdataset3.name)

    crawl_dataset(testdataset3)
    context = Context(testdataset3)
    store = get_store(testdataset3)
    view = store.view(testdataset3)

    with capture_logs() as cap_logs:
        for entity in view.entities():
            check_dangling_references(context, view, entity)

    store.close()
    context.close()

    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert (
        "error: td3-child-of-nonexistent-co property parent references missing id td3-nonexistent-co."
    ) in logs, logs
    assert (
        "error: td3-asset-of-nonexistent-co-ownership-nonexistent-co property owner references missing id td3-nonexistent-co."
    ) in logs, logs
    assert len(logs) == 2, logs


def test_self_references(testdataset3) -> None:
    context = Context(testdataset3)
    store = get_store(testdataset3)
    view = store.view(testdataset3)

    with capture_logs() as cap_logs:
        for entity in view.entities():
            check_self_references(context, view, entity)

    context.close()
    store.close()

    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert (
        "error: td3-owner-of-self-co references itself via ownershipOwner"
        " -> td3-owner-of-self-co-ownership-owner-of-self-co -> asset."
    ) in logs, logs
    assert (
        "error: td3-owner-of-self-co references itself via ownershipAsset"
        " -> td3-owner-of-self-co-ownership-owner-of-self-co -> owner."
    ) in logs, logs
    assert len(logs) == 2, logs
