import os
import zipfile
from tempfile import TemporaryDirectory

from zavod import Context
from zavod.shed.bods import parse_bods_fh


def crawl(context: Context) -> None:
    with TemporaryDirectory() as tmpdir:
        fn = context.fetch_resource("source.zip", context.data_url)
        with zipfile.ZipFile(fn, "r") as zf:
            for name in zf.namelist():
                if not name.endswith(".json"):
                    continue
                target = os.path.join(tmpdir, name)
                if os.path.commonpath([tmpdir, os.path.abspath(target)]) != os.path.abspath(tmpdir):
                    raise ValueError(f"Unsafe path in archive: {name}")
                zf.extract(name, path=tmpdir)
                with open(target, "rb") as fh:
                    parse_bods_fh(context, fh)
                os.unlink(tmpfile)
