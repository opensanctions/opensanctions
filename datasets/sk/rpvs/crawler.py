import zipfile

from zavod import Context
from zavod.shed.bods import parse_bods_fh


def crawl(context: Context) -> None:
    fn = context.fetch_resource("source.zip", context.data_url)
    with zipfile.ZipFile(fn, "r") as zf:
        for name in zf.namelist():
            if not name.endswith(".json"):
                continue
            with zf.open(name, "r") as fh:
                parse_bods_fh(context, fh)
