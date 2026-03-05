from zavod import Context, helpers as h


def crawl_parliament(context: Context, url: str) -> None:
    # doc = context.fetch_json(url)
    breakpoint()


def crawl(context: Context) -> None:
    # locate the link to the list of Portugese parliaments
    doc_landing = context.fetch_html(context.data_url, absolute_links=True)
    url_parliaments = h.xpath_string(
        doc_landing,
        "//a[@title='Recursos' and contains(@href,'DAComposicaoOrgaos')]/@href",
    )

    # iterate over parliaments
    doc_parliament_list = context.fetch_html(url_parliaments, absolute_links=True)
    url = h.xpath_strings(
        doc_parliament_list,
        "//div[@id='ctl00_ctl51_g_48ce9bb1_53ac_4c68_b897_c5870f269772_ctl00_pnlPastas']//a/@href",
    )
    for u in url:
        # get the JSON link
        doc = context.fetch_html(u, absolute_links=True)
        json_url = h.xpath_string(
            doc,
            "//a[starts-with(@title,'OrgaoComposicao') and contains(@title,'_json.txt')]/@href",
        )
        # ^ this wont work for parliament I and one older link, but we also dont want to scrape those

        crawl_parliament(context, json_url)
