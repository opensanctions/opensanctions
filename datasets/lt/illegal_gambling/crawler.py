import re 

from zavod import Context

def crawl_item(item, context: Context):

    name = item.findtext(".//td[2]")
    domain = item.findtext(".//td[3]")
    contacts = item.findtext(".//td[4]")

    if not domain:
        return

    entity = context.make("Organization")
    entity.id = context.make_slug(domain, prefix="lt-illegal-gambling")

    entity.add("name", name)
    entity.add("website", domain)

    # We find all emails in the contacts field and add them to the entity
    emails = re.findall(r"[\w\.-]+@[\w\.-]+", contacts)
    for email in emails:
        entity.add("email", email)

    entity.add("topics", "crime")

    context.emit(entity, target=True)

def crawl(context: Context):

    response = context.fetch_html(context.data_url)

    # We disconsider the two first rows because they are headers
    for item in response.xpath('//*[@class="has-fixed-layout"]/tbody/tr')[2:]:
        crawl_item(item, context)
