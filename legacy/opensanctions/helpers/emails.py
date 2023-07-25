from urllib.parse import unquote
from opensanctions.helpers.text import multi_split


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
