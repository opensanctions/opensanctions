from urllib.parse import urlparse, parse_qs

from lxml.etree import _Element

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract.zyte_api import fetch_html
from zavod.stateful.positions import PositionCategorisation, categorise

# Albanian biography labels in the collapsed ".bio-hidden" block, mapped to the
# FollowTheMoney property they populate. Labels not listed here are passed to
# audit_data() so a new or renamed field fails loudly.
# Date of birth is handled separately (apply_date + backslash normalisation).
DOB_LABEL = "Datëlindja"
BIO_PROPS = {
    "Gjinia": "gender",
    "Përkatësia etnike": "ethnicity",
    "Vendlindja": "birthPlace",
    "Arsimimi": "education",
}
# "Partia" (party) also appears in the bio block, but we take it from the structural
# PARTIA row in ".bio", which is present even when the collapsed bio is empty.
# Bio labels we deliberately drop: no suitable FTM property / not useful for matching.
BIO_IGNORE = [
    "Partia",  # party — taken from the structural PARTIA row instead
    "Statusi civil",  # marital status
    "Gjuhë tjetër përveç amtares",  # other languages spoken
    "Aktivitete dhe funksione paraprake apo të tanishme",  # prior/current occupations
]


def parse_bio(context: Context, doc: _Element) -> dict[str, str]:
    """Parse the "Label: value" biography rows from the collapsed bio block.

    Each field is its own ``<p>`` (e.g. ``Datëlindja: 19/08/1997;``), so we parse per
    paragraph rather than splitting on ";" — a value may itself contain ";" (e.g. a list
    of languages). Minority-community profiles use bilingual ``Albanian\\Serbian`` labels;
    we keep the Albanian (first) half. The block is empty for some deputies, in which case
    an empty dict is returned.
    """
    block = doc.find(".//div[@class='bio-hidden']")
    if block is None:
        return {}
    result: dict[str, str] = {}
    for para in h.xpath_elements(block, "./p"):
        text = h.element_text(para).replace("\xa0", " ").rstrip(";").strip()
        if text == "":
            continue
        if ":" not in text:
            context.log.warning("Bio row without label", row=text)
            continue
        label, value = text.split(":", 1)
        label = label.split("\\")[0].strip()  # drop Serbian half of bilingual labels
        result[label] = value.strip()
    return result


def crawl_member(
    context: Context, position: Entity, categorisation: PositionCategorisation, url: str
) -> None:
    # The site is behind Cloudflare, but Zyte's anti-ban proxy passes it without browser
    # rendering — the HTML is server-rendered, so httpResponseBody returns the full page.
    doc = fetch_html(
        context,
        url,
        unblock_validator=".//h1[@class='name']",
        html_source="httpResponseBody",
        cache_days=20,
    )
    deputy_id = parse_qs(urlparse(url).query)["deputy"][0]

    person = context.make("Person")
    person.id = context.make_slug(deputy_id)
    person.add("name", h.element_text(h.xpath_element(doc, ".//h1[@class='name']")))
    person.add("sourceUrl", url)
    # Deputies of the Assembly must be citizens of Kosovo: Constitution Art. 71(1)
    # (https://www.constituteproject.org/constitution/Kosovo_2016) and Law No. 03/L-073
    # on General Elections (https://www.te.gob.mx/vota_elections/page/download/16375).
    person.add("citizenship", "xk")

    # Party and parliamentary group are labelled <p> rows next to the name. The party
    # is also in the bio block, but those rows are present even when the bio is empty.
    group: str | None = None
    for para in h.xpath_elements(doc, ".//div[@class='bio']/p[span[@class='label']]"):
        label = h.xpath_element(para, "./span[@class='label']")
        label_text = h.element_text(label).rstrip(":").strip()
        value = (label.tail or "").strip()
        if value == "":
            continue
        if label_text == "PARTIA":
            person.add("political", value)
        elif label_text == "GRUPI PARLAMENTAR":
            # politicalGroup is the in-chamber faction, distinct from party membership.
            group = value

    bio = parse_bio(context, doc)
    # Dates use "/" or, on some profiles, "\" as the separator (e.g. 29\03\1970).
    h.apply_date(person, "birthDate", bio.pop(DOB_LABEL, "").replace("\\", "/"))
    for bio_label, prop in BIO_PROPS.items():
        person.add(prop, bio.pop(bio_label, ""))
    context.audit_data(bio, ignore=BIO_IGNORE)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        no_end_implies_current=True,
    )
    if occupancy is None:
        return
    if group is not None:
        occupancy.add("politicalGroup", group)

    context.emit(person)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Assembly of Kosovo",
        country="xk",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q22262242",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    doc = fetch_html(
        context,
        context.data_url,
        unblock_validator=".//a[contains(@href, 'deputydetails')]",
        html_source="httpResponseBody",
        absolute_links=True,
        cache_days=20,
    )
    urls = set()
    for anchor in h.xpath_elements(doc, ".//a[contains(@href, 'deputydetails')]"):
        href = anchor.get("href")
        if href is not None and "deputy=" in href:
            urls.add(href)

    if not urls:
        raise ValueError("No deputy detail links found on the listing page")

    urls_sorted = sorted(urls)
    for index, url in enumerate(urls_sorted, start=1):
        context.log.info(f"Crawling deputy {index}/{len(urls_sorted)}", url=url)
        crawl_member(context, position, categorisation, url)
        # Commit the HTTP cache after each page so a long, interruptible run (every
        # page is a slow Zyte fetch behind Cloudflare) keeps its progress.
        context.flush()
