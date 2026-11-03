import re
from html import unescape

from rigour.mime.types import XML

from zavod import Context
from zavod import helpers as h

# Each feed item describes one researcher and reads, after HTML unescaping:
#   Administrative Actions:<br><ul><li>Debarment (until 06/18/2030)<li>No PHS
#   Advisory (For life)</ul>Memo: Request Retraction of Article(s);
# The <li> elements are never closed, so they are split on the opening tag.
REGEX_ACTION = re.compile(
    r"^(?P<action>.+?)\s*\((?:For life|until\s*(?P<date>[^)]*))\)\s*$",
    re.IGNORECASE,
)


def parse_description(description: str) -> tuple[list[str], str | None]:
    """Split the feed item description into its action strings and the memo."""
    text = unescape(description)
    actions_part, _, memo = text.partition("Memo:")
    _, _, items = actions_part.partition("<ul>")
    items = items.replace("</ul>", "")
    actions = [a.strip() for a in items.split("<li>") if a.strip()]
    return actions, memo.strip() or None


def crawl_item(context: Context, item: dict[str, str | None]) -> None:
    guid = item.pop("guid")
    title = item.pop("title")
    link = item.pop("link")
    if guid is None or title is None:
        context.log.warning("Item without guid or title", item=item)
        return

    # Titles read "Last, First Middle"; the family name can contain spaces.
    last_name, _, first_name = title.partition(",")
    person = context.make("Person")
    person.id = context.make_id(guid)
    h.apply_name(
        person,
        first_name=first_name.strip() or None,
        last_name=last_name.strip(),
    )
    person.add("country", "us")
    person.add("sourceUrl", link)

    actions, memo = parse_description(item.pop("description") or "")
    if not actions:
        context.log.warning("Item without administrative actions", title=title)

    for action in actions:
        match = REGEX_ACTION.match(action)
        if match is None:
            context.log.warning("Cannot parse administrative action", action=action)
            continue
        name = match.group("action")
        # Life terms have no end date; "until " with a blank date has none either.
        end_date = (match.group("date") or "").strip() or None
        sanction = h.make_sanction(
            context,
            person,
            key=name,
            program_key="US-HHS-ORI",
            end_date=end_date,
        )
        sanction.add("provisions", name)
        sanction.add("summary", memo)
        sanction.add("sourceUrl", link)
        if h.is_active(sanction):
            if name == "Debarment":
                person.add("topics", "debarment")
            else:
                person.add("topics", "reg.action")
        context.emit(sanction)

    context.emit(person)
    context.audit_data(item, ignore=["pubDate"])


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.rss", context.data_url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    for node in doc.findall(".//item"):
        item = {child.tag: child.text for child in node}
        crawl_item(context, item)
