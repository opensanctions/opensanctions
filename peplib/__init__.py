import logging


fmt = '[%(levelname)-8s] %(name)-12s: %(message)s'
logging.basicConfig(level=logging.INFO, format=fmt)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('alembic').setLevel(logging.WARNING)
