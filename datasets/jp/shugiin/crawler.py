from urllib.parse import urljoin

from zavod import Context, Entity
from zavod import helpers as h


def crawl_members(context: Context, position: Entity, url: str, name: str) -> None:
    # context.log.info(f"Crawling {url} ({name})")
    doc = context.fetch_html(url, cache_days=7)
    entity = context.make("Person")
    entity.id = context.make_id(url)
    entity.add("name", name)
    entity.add("sourceUrl", url)
    entity.add("topics", "role.pep")
    entity.add("citizenship", "jp")

    contents = doc.find('.//div[@id="contents"]')
    assert contents is not None, "Contents div is missing"
    entity.add("name", contents.findtext(".//h2"))
    notes = contents.findtext('.//p[@class="post"]')
    entity.add("notes", notes)
    prev_label = ""
    for li in contents.findall(".//li"):
        label = h.element_text(li.find(".//strong"))
        if label is not None and len(label):
            prev_label = label
        text = h.element_text(li).replace(label, "").strip()
        if not len(text):
            continue
        elif "In-House Group" in prev_label:
            entity.add("political", text)
        elif "Born" in prev_label:
            if "," in text:
                dob, pob = text.split(",", 1)
            else:
                dob = text
                pob = ""
            h.apply_date(entity, "birthDate", dob.strip())
            entity.add("birthPlace", pob.strip())
            if not entity.has("birthDate"):
                context.log.info("Failed DOB: %r" % text)
        elif "Education" in prev_label:
            entity.add("education", text)
        elif "Career" in prev_label:
            # entity.add("notes", text)
            pass

    occupancy = h.make_occupancy(context, entity, position, no_end_implies_current=True)
    if occupancy is not None:
        context.emit(occupancy)

    context.emit(entity)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Representatives of Japan",
        wikidata_id="Q17506823",
        country="jp",
        topics=["gov.legislative", "gov.national"],
    )

    doc = context.fetch_html(context.data_url)
    urls = [context.data_url]
    for a in doc.findall('.//div[@id="LnaviArea"]//table//a'):
        href = a.get("href")
        if href is None:
            continue
        urls.append(urljoin(context.data_url, href))

    for url in urls:
        doc = context.fetch_html(url)
        for a in doc.findall('.//div[@id="MainContentsArea"]//tr//a'):
            href = a.get("href")
            if href is None:
                continue
            assert a.text is not None, "Link text is missing"
            crawl_members(context, position, urljoin(url, href), a.text)
