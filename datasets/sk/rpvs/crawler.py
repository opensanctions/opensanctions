import logging
import zipfile

import zavod
from zavod import Context
from zavod.shed.bods import parse_bods_fh


def crawl(context: Context) -> None:
    # This crawler emits too many non-actionable warnings, so disable reporting to Sentry for now
    # TODO(Leon Handreke): Clean this up https://github.com/opensanctions/opensanctions/issues/1908
    zavod.logs.set_sentry_event_level(logging.ERROR)

    fn = context.fetch_resource("source.zip", context.data_url)
    with zipfile.ZipFile(fn, "r") as zf:
        for name in zf.namelist():
            if not name.endswith(".json"):
                continue
            with zf.open(name, "r") as fh:
                parse_bods_fh(context, fh)
