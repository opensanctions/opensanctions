from lxml import html
from itertools import count
from urllib.parse import urljoin

from opensanctions.core import Context


def crawl_entry(context: Context, pace, href, member_url):
    _, slug = href.get("href").split("members/", 1)
    person = context.make("Person")
    person.id = context.make_slug(slug)
    for span in href.findall("./span"):
        icon = span.find(".//i")
        if icon is None:
            icon = span.find("./span")
        if icon is None:
            context.log.warning("Empty span", span=span, entity=person)
            continue
        category = icon.get("class")
        text = icon.tail or span.text_content()
        if "fa-user" in category:
            text = text.strip()
            title = context.lookup_value("title", text)
            if title is not None:
                person.add("title", title)
                text = text[len(title) :]
            else:
                context.log.info("No title in name", value=text, entity=person)
                return
            person.add("name", text)
        elif "flag-icon" in category:
            person.add("nationality", text)
        elif "circle-thin" in category:
            person.add("political", text)
        elif "fa-calendar" in category:
            member = context.make("Membership")
            member.id = context.make_slug(f"{slug}-pace")
            member.add("organization", pace)
            member.add("member", person)
            joined = text.replace("Joined in ", "")
            if "," in joined:
                joined, left = joined.split(", left in")
                member.add("endDate", left)
            member.add("startDate", joined)
            await context.emit(member)
        else:
            context.log.warning("Unknown category", span=span, entity=person)

    person.add("sourceUrl", member_url)
    person.add("topics", "role.pep")
    if not person.has("name"):
        context.log.warning("No name on entity", entity=person)
    await context.emit(person, target=True, unique=True)


def crawl(context: Context):
    index_url = context.dataset.data.url
    pace = context.make("PublicBody")
    pace.id = context.make_slug("pace")
    pace.add("name", "Counil of Europe Parliamentary Assembly")
    await context.emit(pace)
    for page_idx in count(1):
        context.log.debug("Members directory", page=page_idx)
        params = {"page": page_idx}
        res = context.http.get(index_url, params=params)
        doc = html.fromstring(res.text)
        page_empty = True
        for article in doc.findall(".//article"):
            href = article.find("./p/a")
            if href is None:
                continue
            member_url = urljoin(index_url, href.get("href"))
            if "/members/" not in member_url:
                continue
            page_empty = False
            crawl_entry(context, pace, href, member_url)

        if page_empty:
            break
