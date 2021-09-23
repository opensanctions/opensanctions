from urllib.parse import urljoin
from normality import stringify, collapse_spaces, slugify
from lxml import html
from prefixdate.formats import parse_format

from opensanctions.helpers import make_address, apply_address


def parse_date(text):
    return parse_format(text, "%d/%m/%Y")


def crawl_person(context, name, url):
    context.log.debug("Crawling member", name=name, url=url)
    res = context.http.get(url)
    doc = html.fromstring(res.text)
    _, person_id = url.rsplit("/", 1)
    person = context.make("Person")
    person.make_slug(person_id)
    person.add("sourceUrl", url)
    person.add("name", name)
    person.add("topics", "role.pep")

    last_name, first_name = name.split(", ", 1)
    person.add("firstName", first_name)
    person.add("lastName", last_name)

    address = {}
    details = doc.find('.//div[@class="regular-details"]')
    for row in details.findall('.//ul[@class="no-bullet"]/li'):
        children = row.getchildren()
        title = children[0]
        title_text = collapse_spaces(stringify(title.text_content()))
        title_text = title_text or title.get("class")
        value = collapse_spaces(title.tail)
        if title_text in ("Full name:", "Address:", "Declaration of interests"):
            # ignore these.
            continue
        if title_text == "Emails:":
            emails = [e.text for e in row.findall(".//a")]
            person.add("email", emails)
            continue
        if "glyphicon-phone" in title_text:
            person.add("phone", value.split(","))
            continue
        if "fa-fax" in title_text:
            # TODO: yeah, no
            # person.add("phone", value)
            continue
        if title_text in ("Web sites:", "list-inline"):
            sites = [e.get("href") for e in row.findall(".//a")]
            person.add("website", sites)
            continue
        if title_text == "Represented Country:":
            person.add("country", value)
            continue
        if title_text == "Languages:":
            # TODO: missing in FtM
            # person.add("languages", value.split(','))
            continue
        if "Regions since:" in title_text:
            date = parse_date(value)
            person.context["created_at"] = date
            continue
        if "Date of birth:" in title_text:
            person.add("birthDate", parse_date(value))
            continue
        if "Commissions:" in title_text:
            for com in row.findall(".//li"):
                text = collapse_spaces(com.text_content())
                sep = "Mandate - "
                if sep in text:
                    _, text = text.split(sep, 1)
                person.add("sector", text)
            continue
        if "Areas of interest:" in title_text:
            for area in row.findall(".//li"):
                person.add("keywords", area.text_content())
            continue
        if title.tag == "i" and value is None:
            person.add("position", title_text)
            continue
        if title_text in ("Country:"):
            person.add("country", value)
        if title_text in ("Street:", "Postal code:", "City:", "Country:"):
            address[title_text.replace(":", "")] = value
            continue
        if title_text == "Political group:":
            group = context.make("Organization")
            group.add("name", value)
            slug = value
            if "(" in slug:
                _, slug = slug.rsplit("(", 1)
            slug = slugify(slug, sep="-")
            group.id = f"eu-cor-group-{slug}"
            context.emit(group)
            member = context.make("Membership")
            member.make_id("Membership", person.id, group.id)
            member.add("member", person)
            member.add("organization", group)
            context.emit(member)
            continue

    address = make_address(
        context,
        street=address.get("Street"),
        city=address.get("City"),
        postal_code=address.get("Posal code"),
        country=address.get("Country"),
    )
    apply_address(context, person, address)
    context.emit(person, target=True)


def crawl(context):
    res = context.http.get(context.dataset.data.url)
    doc = html.fromstring(res.text)

    seen = set()
    for link in doc.findall('.//div[@class="people"]//li//a[@class="_fullname"]'):
        url = urljoin(res.url, link.get("href"))
        url, _ = url.split("?", 1)
        if url in seen:
            continue
        seen.add(url)
        crawl_person(context, link.text, url)
