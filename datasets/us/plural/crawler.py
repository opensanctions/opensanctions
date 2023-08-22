from zipfile import ZipFile
from pantomime.types import ZIP
from yaml import safe_load
from typing import Any
import re

from zavod.context import Context
from zavod import helpers as h


REGEX_PATH = re.compile("^people-main/data/(?P<state>[a-z]{2})/(?P<body>legislature|executive)")
def crawl_person(context, body: str, data: dict[str, Any]):
    print(data.pop("name"), body)


def crawl(context: Context):
    path = context.fetch_resource("source.zip", context.data_url)
    context.export_resource(path, ZIP, title=context.SOURCE_TITLE)
    with ZipFile(path) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            match = REGEX_PATH.match(member.filename)
            if match and match.group("state") != "us":
                with archive.open(member) as filestream:
                    print(member.filename)
                    crawl_person(context, match.group("body"), safe_load(filestream))