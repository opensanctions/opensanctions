import logging

__version__ = "3.0.0"

logging.getLogger("requests_cache").setLevel(logging.WARNING)
logging.getLogger("alembic").setLevel(logging.WARNING)
