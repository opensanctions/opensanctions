from zavod import Context, helpers as h


def crawl(context: Context):

    doc = context.fetch_html(context.data_url, cache_days=2)
    # Get necessary form fields
    viewstate = doc.xpath('//input[@name="__VIEWSTATE"]/@value')[0]
    eventvalidation = doc.xpath('//input[@name="__EVENTVALIDATION"]/@value')[0]

    # Prepare form data for POST request
    form_data = {
        "__VIEWSTATE": viewstate,
        "__EVENTVALIDATION": eventvalidation,
    }

    results_doc = context.fetch_html(
        context.data_url,
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
