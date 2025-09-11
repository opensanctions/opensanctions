from zavod import Context, helpers as h

BODY_EXCLUDED_URLS = {
    "https://www.mas.gov.sg/regulation/enforcement/enforcement-actions/2022/mas-obtains-civil-penalty-default-judgment-against-mr-liao-chun-te-for-insider-trading",
    "https://www.mas.gov.sg/regulation/enforcement/enforcement-actions/2022/mas-penalises-vistra-for-failures-in-aml-controls",
    "https://www.mas.gov.sg/regulation/enforcement/enforcement-actions/2021/mas-bans-former-representative-of-insurance-broker-for-dishonest-conduct",
}

ARTICLE_XPATH = "//div[contains(@class, 'mas-section__banner-item')]"
SUMMARY_XPATH = ".//div[contains(@class, 'mas-text-summary mas-rte-content')]"
BODY_XPATH = (
    ".//div[contains(@class, '_mas-typeset') and contains(@class, 'mas-rte-content')]"
)


def crawl_enforcement_action(context: Context, url: str, date: str, action_type: str):
    article = context.fetch_html(url, cache_days=7)
    article.make_links_absolute(context.data_url)
    article_full = article.xpath(ARTICLE_XPATH)
    assert len(article_full) == 1, "Expected exactly one article in the document"
    article_name = article_full[0].xpath("./h1")
    assert len(article_name) == 1, "Expected exactly one article title in the document"
    article_summary = article_full[0].xpath(SUMMARY_XPATH)
    assert len(article_summary) == 1, "Expected exactly one article summary"
    article_body = article_full[0].xpath(BODY_XPATH)
    if url not in BODY_EXCLUDED_URLS:
        assert len(article_body) == 1, "Expected exactly one article body"

    # Extract text safely, strip, and merge
    article_text = "\n\n".join(
        element.text_content().strip()
        for element in (article_summary[:1] + article_body[:1])
        if element is not None and element.text_content().strip()
    )
    assert article_text, "Expected non-empty article text"


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=7)
    doc.make_links_absolute(context.data_url)
    table = doc.xpath("//table")
    assert len(table) == 1, "Expected exactly one table in the document"
    for row in h.parse_html_table(table[0]):
        links = h.links_to_dict(row.pop("title"))
        str_row = h.cells_to_str(row)
        date = str_row.pop("issue_date")
        entities = str_row.pop("person_company")
        action_type = str_row.pop("action_type")
        context.audit_data(str_row)
        url = next(iter(links.values()))
        crawl_enforcement_action(context, url, date, action_type)
