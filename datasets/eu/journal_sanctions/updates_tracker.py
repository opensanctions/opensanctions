import re

from zavod import Context
from zavod import helpers as h

REGIME_URL = "https://www.sanctionsmap.eu/api/v1/regime"

EUR_LEX_REGEX = re.compile(r"https://eur-lex\.europa\.eu/legal-content/.*")

CELEX_DATE_REGEX = re.compile(
    r"https:\/\/eur-lex\.europa\.eu\/legal-content\/EN\/TXT\/\?uri=CELEX%3A[A-Z0-9]+-[0-9]{8}"
)
OJ_REGEX = re.compile(
    r"https:\/\/eur-lex\.europa\.eu\/legal-content\/EN\/TXT\/\?uri=OJ:[A-Z]_[0-9]{4}[0-9]{3}"
)

# outliers:
# https://eur-lex.europa.eu/legal-content/EN/TXT/?qid=1571925577358&uri=CELEX:01996R2271-20180807
# https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=uriserv:OJ.LI.2018.199.01.0007.01.ENG&toc=OJ:L:2018:199I:TOC


OJ_URLS = [
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202402113",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202401968",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202402075",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202402074",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202401793",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202401971",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202302287",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202302406",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202401484",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202401776",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202402055",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202402056",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=OJ:L_202402207",
]

CELEX_URLS = [
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011D0486-20220205",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011R0753-20220413",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02006R0765-20240701",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02012D0642-20240805",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011D0173-20240327",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02015D1763-20231129",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02015R1755-20231129",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014R0224-20231111",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02013D0798-20231111",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02018D1544-20240717",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02018R1542-20240717",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02019R0796-20240624",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02019D0797-20240624",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02016D0849-20240730",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02017R1509-20240730",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02005R1183-20240726",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02010D0788-20240726",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02024D0254-20240202",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02024R0287-20240202",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02010D0638-20231129",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02009R1284-20231129",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02012D0285-20230805",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02012R0377-20230805",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02022D2319-20240626",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02022R2309-20240626",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02020D1999-20240722",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02020R1998-20240722",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011R0359-20240626",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011D0235-20240626",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02012R0267-20231018",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02010D0413-20231018",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02023R1529-20240531",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02023D1532-20240717",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02003E0495-20230216",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02003R1210-20231013",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02006R1412-20220413",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02005E0888-20230216",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02006R0305-20230216",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02021R1275-20231129",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02021D1277-20240717",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02016R0044-20240724",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02015D1333-20240724",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02017R1770-20240424",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02017D1775-20240424",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02023D0891-20240430",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02023R0888-20240430",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02010D0573-20231028",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02013R0401-20240430",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02013D0184-20240430",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02019R1716-20231129",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02019D1720-20231129",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02024R1485-20240527",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014D0512-20240724",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014R0833-20240625",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02010D0231-20240617",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02010R0356-20240617",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02003R0147-20240320",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02015D0740-20240619",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02015R0735-20240619",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014D0450-20230404",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014R0747-20230404",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02023R2147-20240624",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02023D2135-20240624",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02013D0255-20240529",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02012R0036-20240529",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02006R0305-20230216",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02005E0888-20230216",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02001E0931-20240221",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02001R2580-20240221",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02016R1686-20240325",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02016D1693-20240325",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02002R0881-20240507",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02024R0386-20240628",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02024D0385-20240628",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011R0101-20240131",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011D0072-20240131",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02019D1894-20231111",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02019R1890-20231111",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014D0119-20240306",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014R0208-20240306",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02022R0263-20221007",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02022D0266-20240221",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014D0145-20240628",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014R0269-20240628",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014D0386-20240619",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02017R2063-20240515",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02017D2074-20240515",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014R1352-20230216",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014D0932-20230215",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011D0101-20240206",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02004R0314-20231129",
]


def crawl(context: Context):
    regime = context.fetch_json(REGIME_URL, cache_days=1)
    for item in regime["data"]:
        regime_url = f"{REGIME_URL}/{item['id']}"
        regime_data = context.fetch_json(regime_url, cache_days=1)["data"]
        legal_acts = regime_data.pop("legal_acts", None)

        existing_urls = set(CELEX_URLS)
        new_url_found = False
        date_changed = False
        logged_urls = set()  # Track URLs that have already been logged

        for item in regime["data"]:
            regime_url = f"{REGIME_URL}/{item['id']}"
            regime_data = context.fetch_json(regime_url, cache_days=1)["data"]
            legal_acts = regime_data.pop("legal_acts", None)

            if legal_acts:
                for act in legal_acts["data"]:
                    url = act.get("url")
                    if url and CELEX_DATE_REGEX.match(url):
                        # Extract the last 8 characters (date) from the URL
                        date = url[-8:]
                        url_without_date = url[:-8]

                        # Find a matching URL by ignoring the last 8 characters (date)
                        matched_existing_url = next(
                            (
                                _url
                                for _url in existing_urls
                                if _url[:-8] == url_without_date
                            ),
                            None,
                        )

                        if matched_existing_url is None and (url not in logged_urls):
                            context.log.warning(f"New URL found: {url}")
                            new_url_found = True
                            logged_urls.add(url)
                        else:
                            existing_date = matched_existing_url[-8:]
                            if existing_date != date and url not in logged_urls:
                                context.log.warning(
                                    f"Date changed for URL: {matched_existing_url}. New date: {date}"
                                )
                                logged_urls.add(url)
                                date_changed = True

        # Log a confirmation message if no discrepancies were found
        if not new_url_found and not date_changed:
            context.log.info("All URLs are the same and dates match.")
