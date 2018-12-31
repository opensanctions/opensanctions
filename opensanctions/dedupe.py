from alephclient.api import AlephAPI
from followthemoney import model

from opensanctions import settings
from opensanctions.model import Entity, Session


def iter_entities():
    session = Session()
    for entity in Entity.all(session):
        yield entity.proxy.to_dict()


def load_collection(api, foreign_id):
    collections = api.filter_collections(filters=[('foreign_id', foreign_id)])
    for collection in collections:
        return collection.get('id')

    data = {
        'foreign_id': foreign_id,
        'label': foreign_id,
        'category': 'other'
    }
    collection = api.create_collection(data)
    return collection.get('id')


def get_api():
    return AlephAPI(settings.ALEPH_HOST, settings.ALEPH_API_KEY)


def load_aleph():
    api = get_api()
    collection_id = load_collection(api, settings.ALEPH_COLLECTION)
    api.bulk_write(collection_id, iter_entities())


def match_entities():
    api = get_api()
    session = Session()
    collection_id = load_collection(api, settings.ALEPH_COLLECTION)
    for entity in Entity.all(session):
        if not entity.proxy.schema.matchable:
            continue
        data = entity.proxy.to_dict()
        for match in api.match(data, collection_ids=[collection_id]):
            proxy = model.get_proxy(match)
            Entity.save(session, 'aleph', proxy)
            print(repr(entity.proxy), repr(proxy))


if __name__ == '__main__':
    # load_aleph()
    match_entities()
