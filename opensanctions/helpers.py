from followthemoney import model

from opensanctions.model import Session, Entity


def store_entity(context, data):
    proxy = model.get_proxy(data)
    session = Session()
    Entity.save(session, context.crawler.name, proxy)
    session.commit()
    session.close()
