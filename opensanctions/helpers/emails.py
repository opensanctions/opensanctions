from banal import ensure_list
from urllib.parse import unquote
from opensanctions.util import multi_split


def clean_emails(emails):
    out = []
    for email in multi_split(emails, ["/", ","]):
        if email is None:
            return
        email = unquote(email)
        email = email.strip()
        email = email.rstrip(".")
        out.append(email)
    return out
