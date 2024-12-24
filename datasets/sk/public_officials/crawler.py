from zavod import Context, helpers as h

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Content-Length": "42154",
    "Content-Type": "application/x-www-form-urlencoded",
    "Cookie": "ASP.NET_SessionId=2zclslyuzwm11i552k3ymtb0",
    "Host": "www.nrsr.sk",
    "Origin": "https://www.nrsr.sk",
    "Referer": "https://www.nrsr.sk/web/Default.aspx?sid=vnf%2fzoznam&ViewType=2",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
}


def crawl(context: Context):

    doc = context.fetch_html(context.data_url, cache_days=2)

    options = doc.xpath('//*[@id="_sectionLayoutContainer_ctl01_FunkcieList3x"]/option')
    print(f"Found {len(options)} options")
    for option in options:
        value = option.get("value")  # Get the value of the option
        label = option.text_content().strip()  # Get the text label of the option
        print(f"Found option: {label} (value={value})")

        context.log.info(f"Querying for option: {label} (value={value})")

    form_data = {
        "MIME Type": "application/x-www-form-urlencoded",
        "__VIEWSTATEGENERATOR": "DB1B4C9A",
        "_sectionLayoutContainer$ctl01$FunkcieList3x": "44",  # Set the dropdown value
        "_sectionLayoutContainer$ctl01$ShowSelectedView": "Zobraziť",
        "_sectionLayoutContainer$ctl00$_calendarYear": "2024",
        "_sectionLayoutContainer$ctl00$_calendarMonth": "12",
        "_sectionLayoutContainer$ctl00$_calendarApp": "nrdvp",
        "_sectionLayoutContainer$ctl00$_monthSelector": "12",
        "_sectionLayoutContainer$ctl00$_yearSelector": "2024",
    }

    # Fetch the results page for the current dropdown value
    results_doc = context.fetch_html(
        context.data_url,
        headers=HEADERS,
        method="POST",
        data=form_data,
        cache_days=2,
    )
    links = results_doc.xpath(
        '//div[@id="_sectionLayoutContainer__panelContent"]//a[@href]'
    )
    for link in links:
        results_doc.make_links_absolute(context.data_url)  # Make the link absolute
        href = link.get("href")  # Get the href value
        link_text = link.text_content().strip()  # Get the link text
        context.log.info(f"Found link: {link_text} (href={href})")
