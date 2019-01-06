import json
from datetime import datetime
from followthemoney import model
from memorious.core import datastore

from opensanctions import settings


def get_table():
    return datastore.get_table(settings.TABLE, primary_id=False)


def store_entity(context, data):
    proxy = model.get_proxy(data)
    if proxy.id is None:
        return
    table = get_table()
    table.upsert({
        'id': proxy.id,
        'schema': proxy.schema.name,
        'crawler': context.crawler.name,
        'properties': json.dumps(proxy.properties),
        'context': json.dumps(proxy.context),
        'updated_at': datetime.utcnow()
    }, ['id'])


def iter_entities(crawler=None):
    table = get_table()
    if crawler is not None:
        table = table.find(crawler=crawler)
    for row in table:
        data = json.loads(row.get('context'))
        data['id'] = row.get('id')
        data['schema'] = row.get('schema')
        data['crawler'] = row.get('crawler')
        data['properties'] = json.loads(row.get('properties'))
        yield data
