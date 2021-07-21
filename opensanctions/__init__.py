import logging

logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("requests_cache").setLevel(logging.WARNING)
# logging.getLogger("alembic").setLevel(logging.WARNING)
logging.getLogger("httpstream").setLevel(logging.WARNING)
logging.getLogger("prefixdate").setLevel(logging.ERROR)
