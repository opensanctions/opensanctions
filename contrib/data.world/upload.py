# pip install datadotworld requests
from pprint import pprint
import requests
import datadotworld as dw
from datadotworld.client.api import RestApiError

SUMMARY = '''
%(summary)s

See: %(url)s

This dataset is part of the OpenSanctions project: https://opensanctions.org/docs/about
'''
INDEX_URL = 'https://data.opensanctions.org/datasets/latest/index.json'
USER = 'opensanctions'
client = dw.api_client()


def truncate(text, max_len=110):
    if len(text) <= max_len:
        return text, ''
    prefix = text[:max_len]
    prefix, _ = prefix.rsplit(' ', 1)
    return prefix + '...', ''


def transfer_dataset(dataset):
    key = dataset.get('name').replace('_', '-')
    if key in ('all',):
        return
    try:
        client.create_dataset(USER, title=key, visibility='OPEN')
    except RestApiError as error:
        if error.status != 400:
            print(error)
    key = "%s/%s" % (USER, key)
    pprint(key)
    description, summary = truncate(dataset.get('summary'))
    url = 'https://opensanctions.org/datasets/%s/' % dataset.get('name')
    summary = SUMMARY % {'summary': summary, 'url': url}
    client.update_dataset(key,
        title=dataset.get('title'),
        description=description,
        summary=summary.strip(),
        license='CC-BY',
        files={},
    )

    for resource in dataset.get('resources', []):
        if resource.get('path') != 'targets.simple.csv':
            client.delete_files(key, [resource.get('path')])
            continue
        # pprint(resource)
        client.add_files_via_url(key, {
            resource.get('path'): {
                'url': resource.get('url'),
                'description': resource.get('title')
            }
        })


def transfer_datasets():
    data = requests.get(INDEX_URL).json()
    for dataset in data.get('datasets', []):
        transfer_dataset(dataset)


if __name__ == '__main__':
    transfer_datasets()