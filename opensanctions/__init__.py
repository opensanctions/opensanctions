import logging
import warnings
import urllib3

logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("charset_normalizer").setLevel(logging.WARNING)
logging.getLogger("httpstream").setLevel(logging.WARNING)
logging.getLogger("prefixdate").setLevel(logging.ERROR)

warnings.simplefilter("ignore", category=ResourceWarning)
urllib3.disable_warnings()
