import requests
from zavod import Context, helpers as h

QUERY_URL = " https://www.irishstatutebook.ie/solr/all_leg_title/select?q=unlawful+organisation&rows=10&hl.maxAnalyzedChars=-1&sort=year+desc&facet=true&facet.field=year&facet.field=type&facet.limit=300&facet.mincount=1&json.nl=map&wt=json"

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


def fetch_new_links(context: Context) -> None:
    response = requests.get(QUERY_URL)
    data = response.json()

    # Check the 'numFound' field in the response
    num_found = data.get("response", {}).get("numFound", 0)
    if num_found > 2:
        context.log.error(f"numFound is greater than 2: {num_found}")
        raise Exception("Number of found documents is greater than 2.")


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
        entity.add("topics", "sanction")
        entity.add("sourceUrl", nro["url"])
        context.emit(entity)

        sanction = h.make_sanction(context, entity)
        context.emit(sanction)


def crawl(context: Context):
    fetch_new_links(context)
    process_hardcoded_data(context)
