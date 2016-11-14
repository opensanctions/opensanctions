import os

DATA_FIXTURES = os.path.join(os.path.dirname(__file__), 'data')
SCHEMA_FIXTURES = os.path.join(os.path.dirname(__file__), 'schema')
DATABASE_URI = os.environ.get('DATABASE_URI')