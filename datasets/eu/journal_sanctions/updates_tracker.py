import re

from zavod import Context
from zavod import helpers as h

REGIME_URL = "https://www.sanctionsmap.eu/api/v1/regime"

EUR_LEX_REGEX = re.compile(r"https://eur-lex\.europa\.eu/legal-content/.*")

EXPECTED_HASHES = {
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02017D2074-20240515": "5e1b7e2a082e5dee206be5c947f081417959f719",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014R1352-20230216": "14737c11576c8ce30067b1f113ed3896fc4951b5",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02014D0932-20230216": "9b02ef871c837b7ecb4a4f64cb748a9d7842b723",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011D0101-20240206": "28d86979b1116e70271ded46fbcd1ff47b26f946",
    "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02004R0314-20231129": "be04d08bd69d46027a3b8d7c365ffcd395ddb669",
}


def crawl(context: Context):
    regime = context.fetch_json(REGIME_URL, cache_days=1)
    for item in regime["data"]:
        regime_url = f"{REGIME_URL}/{item['id']}"
        regime_data = context.fetch_json(regime_url, cache_days=1)["data"]
        legal_acts = regime_data.pop("legal_acts", None)
        for act in legal_acts["data"]:
            url = act.get("url")
            if url and EUR_LEX_REGEX.match(url):
                expected_hash = EXPECTED_HASHES.get(url)
                if expected_hash:
                    # Check the URL hash
                    h.assert_url_hash(context, url, expected_hash)
                else:
                    context.log.warning(f"No expected hash found for URL: {url}")
        # print(url)

        # h.assert_url_hash(regime_url)
