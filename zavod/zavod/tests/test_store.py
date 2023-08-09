from zavod import settings
from zavod.meta import Dataset
from zavod.crawl import crawl_dataset
from zavod.store import get_store, get_view, clear_store


def test_store_access(testdataset1: Dataset):
    crawl_dataset(testdataset1)
    store = get_store(testdataset1, external=True)
    view = store.default_view(external=True)
    assert len(list(view.entities())) > 5, list(view.entities())
    entity = view.get_entity("osv-john-doe")
    assert entity is not None, entity
    assert entity.id == "osv-john-doe"
    store.close()

    view2 = get_view(testdataset1, external=True)
    entity = view2.get_entity("osv-john-doe")
    assert entity is not None, entity
    assert entity.id == "osv-john-doe"
    assert entity.schema.name == "Person"
    assert entity.target is True
    # assert entity.external is False
    assert entity.last_change == settings.RUN_TIME_ISO
    assert entity.first_seen == settings.RUN_TIME_ISO
    assert entity.last_seen == settings.RUN_TIME_ISO
    view2.store.close()
    clear_store(testdataset1)
    assert not store.path.exists()
