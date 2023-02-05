import re
from banal import is_listish
from typing import Optional, List, Union
from normality import collapse_spaces

PREFIX_ = "INTERPOL-UN\s*Security\s*Council\s*Special\s*Notice\s*web\s*link:?"
PREFIX = re.compile(PREFIX_, re.IGNORECASE)

INTERPOL_URL_ = "https?:\/\/www\.interpol\.int\/[^ ]*(\s\d+)?"
INTERPOL_URL = re.compile(INTERPOL_URL_, re.IGNORECASE)


def clean_note(text: Union[Optional[str], List[Optional[str]]]) -> List[str]:
    out: List[str] = []
    if text is None:
        return out
    if is_listish(text):
        for t in text:
            out.extend(clean_note(t))
        return out
    text = PREFIX.sub(" ", text)
    text = INTERPOL_URL.sub(" ", text)
    text = collapse_spaces(text)
    if text is None:
        return out
    return [text]
