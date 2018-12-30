import os

DATABASE_URI = 'sqlite:///opensanctions.sqlite'
DATABASE_URI = os.environ.get('OPENSANCTIONS_DATABASE_URI', DATABASE_URI)
