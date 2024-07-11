from lxml import html
import requests
from urllib.parse import urljoin
from zavod import Context, helpers as h

SEARCH_URL = "https://www.irishstatutebook.ie/eli/ResultsTitle.html?q=unlawful+organisation&search_type=all"
INITIAL_LINKS = [
    # "https://www.irishstatutebook.ie/eli/1983/si/7/made/en/print?q=unlawful+organisation&search_type=all",
    # "https://www.irishstatutebook.ie/eli/1939/sro/162/made/en/print?q=unlawful+organisation&search_type=all"  # ,
]

HARD_CODED_DATA = [
    {
        "url": "https://www.irishstatutebook.ie/eli/1983/si/7/made/en/print?q=unlawful+organisation&search_type=all",
        "title": "S.I. No. 7/1983 - Unlawful Organisation (Suppression) Order, 1983.",
        "sections": [
            {
                "section_title": "1. Citation",
                "section_content": [
                    "This Order may be cited as the Unlawful Organisation (Suppression) Order, 1983."
                ],
            },
            {
                "section_title": "2. Declaration",
                "section_content": [
                    "It is hereby declared that the organisation styling itself the Irish National Liberation Army (also the I.N.L.A.) is an unlawful organisation and ought, in the public interest, to be suppressed."
                ],
            },
            {
                "section_title": "3. Organization",
                "section_content": ["Irish National Liberation Army"],
            },
            {
                "section_title": "4. Alias",
                "section_content": ["I.N.L.A"],
            },
        ],
    },
    {
        "url": "https://www.irishstatutebook.ie/eli/1939/sro/162/made/en/print?q=unlawful+organisation&search_type=all",
        "title": "S.I. No. 162/1939 - Unlawful Organisation (Suppression) Order, 1939.",
        "sections": [
            {
                "section_title": "1. Citation",
                "section_content": [
                    "This Order may be cited as the Unlawful Organisation (Suppression) Order, 1939."
                ],
            },
            {
                "section_title": "2. Declaration",
                "section_content": [
                    "It is hereby declared that the organisation styling itself the Irish Republican Army (also the I.R.A. and Oglaigh na hÉireann) is an unlawful organisation and ought, in the public interest, to be suppressed."
                ],
            },
            {
                "section_title": "3. Organization",
                "section_content": ["Irish Republican Army"],
            },
            {
                "section_title": "4. Alias",
                "section_content": ["I.R.A.", "Oglaigh na héireann"],
            },
        ],
    },
]


def fetch_new_links(context: Context) -> set:
    response = requests.get(SEARCH_URL)
    page_content = html.fromstring(response.content)
    links = page_content.xpath(
        '//*[contains(concat(" ", @class, " "), concat(" ", "result", " ")) and (((count(preceding-sibling::*) + 1) = 2) and parent::*)]//a/@href'
    )
    if isinstance(links, list):
        full_links = {
            urljoin("https://www.irishstatutebook.ie", str(link)) for link in links
        }
    else:
        full_links = set()

    new_links = full_links - set(INITIAL_LINKS)
    if new_links:
        context.log.error(f"New links found: {new_links}")
        raise Exception("New links found that are not in the INITIAL_LINKS.")
    return new_links


def process_hardcoded_data(context: Context):
    for nro in HARD_CODED_DATA:
        data = {
            sec["section_title"]: list(sec["section_content"])
            for sec in nro["sections"]
        }
        entity = context.make("Organization")
        entity.id = context.make_id("nro", data["3. Organization"][0])
        entity.add("name", data["3. Organization"][0])
        entity.add("country", "Ireland")
        entity.add("alias", data["4. Alias"])
        entity.add("notes", data["2. Declaration"][0])
        entity.add("topics", "export.control")
        entity.add("sourceUrl", nro["url"])
        context.emit(entity, target=True)

        sanction = h.make_sanction(context, entity)
        context.emit(sanction)


def crawl(context: Context):
    try:
        new_links = fetch_new_links(context)
        if new_links:
            raise Exception("New links found!")
    except Exception as e:
        context.log.error(f"Exception: {e}")
    else:
        process_hardcoded_data(context)
