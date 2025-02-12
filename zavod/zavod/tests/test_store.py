from zavod import settings
from zavod.integration import get_dataset_linker
from zavod.meta import Dataset
from zavod.crawl import crawl_dataset
from zavod.store import get_store


def test_store_access(testdataset1: Dataset):
    linker = get_dataset_linker(testdataset1)
    crawl_dataset(testdataset1)
    store = get_store(testdataset1, linker)
    store.sync()
    view = store.default_view(external=True)
    assert len(list(view.entities())) > 5, list(view.entities())
    entity = view.get_entity("osv-john-doe")
    assert entity is not None, entity
    assert entity.id == "osv-john-doe"
    store.close()

    store = get_store(testdataset1, linker)
    store.sync()
    view2 = store.view(testdataset1, external=False)
    entity = view2.get_entity("osv-john-doe")
    assert entity is not None, entity
    assert entity.id == "osv-john-doe"
    assert entity.schema.name == "Person"
    # assert entity.external is False
    assert entity.last_change == settings.RUN_TIME_ISO
    assert entity.first_seen == settings.RUN_TIME_ISO
    assert entity.last_seen == settings.RUN_TIME_ISO
    view2.store.close()
    store = get_store(testdataset1, linker)
    store.clear()
    empty = store.view(testdataset1, external=False)
    assert len(list(empty.entities())) == 0
