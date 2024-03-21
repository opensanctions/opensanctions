# dangling reference on a thing
# dangling reference on an interval
# direct self reference - FTP already prevents this
# self reference 1 level deep
from json import load
from structlog import get_logger
from structlog.testing import capture_logs

from zavod.context import Context
from zavod.store import get_store 
from zavod.verify import check_dangling_references, check_self_references
from zavod.archive import clear_data_path
from zavod.crawl import crawl_dataset
from zavod import settings


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
    logs = [f"{l['log_level']}: {l['event']}" for l in cap_logs]
    assert (
        "error: td3-child-of-nonexistent-co property parent references missing id td3-nonexistent-co"
    ) in logs, logs 
    assert (
        "td3-asset-of-nonexistent-co-ownership-nonexistent-co property owner references missing id td3-nonexistent-co"
    ) in logs, logs 


def test_self_references(testdataset3) -> None:
    context = Context(testdataset3)
    store = get_store(testdataset3)
    view = store.view(testdataset3)

    for entity in view.entities():
        check_self_references(context, view, entity)

    context.close()
    store.close()
