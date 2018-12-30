from followthemoney import model
from opensanctions.store.model import Session, Entity


def store_entity(context, data):
    from pprint import pprint
    pprint(data)
    proxy = model.get_proxy(data)
    session = Session()
    Entity.save(session, context.crawler.name, proxy)
    session.commit()
    session.close()
