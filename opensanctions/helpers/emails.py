from banal import ensure_list


def clean_emails(emails):
    out = []
    for email in ensure_list(emails):
        if email is None:
            return
        email = email.strip()
        email = email.rstrip(".")
        out.append(email)
    return out
