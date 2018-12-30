import os

DATABASE_URI = 'sqlite:///opensanctions.sqlite'
DATABASE_URI = os.environ.get('OPENSANCTIONS_DATABASE_URI', DATABASE_URI)

ALEPH_HOST = os.environ.get('OPENSANCTIONS_ALEPH_HOST')
ALEPH_API_KEY = os.environ.get('OPENSANCTIONS_ALEPH_API_KEY')
ALEPH_COLLECTION = os.environ.get('OPENSANCTIONS_ALEPH_COLLECTION', 'opensanctions')  # noqa