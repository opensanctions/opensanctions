from zavod import Context


# LINKS = [
#     f"https://war-sanctions.gur.gov.ua/en/kidnappers/persons?page={page}&per-page=12",  # CHILD KIDNAPPERS Persons
#     f"https://war-sanctions.gur.gov.ua/en/kidnappers/companies?page={page}&per-page=12s",  # CHILD KIDNAPPERS Legal Entities
# ]


def crawl(context: Context):
    index_page = context.fetch_html(context.data_url, cache_days=3)

    main_grid = index_page.find('.//div[@id="main-grid"]')
    if main_grid is not None:
        for a in main_grid.findall(".//a"):
            href = [a.get("href")]
            for link in href:
                if link.startswith("https:"):
                    detail_page = context.fetch_html(link, cache_days=3)

                    details_container = detail_page.find(
                        ".//div[@id='js_visibility'][@class='col-12 col-lg-9']"
                    )
                    if details_container is None:
                        context.log.warning(
                            f"Could not find details container on {link}"
                        )
                        continue
                    data = {}
                    for row in details_container.findall(".//div[@class='row']"):
                        label_elem = row.find(
                            ".//div[@class='col-12 col-md-4 col-lg-2 yellow']"
                        )
                        value_elem = row.find(
                            ".//div[@class='col-12 col-md-8 col-lg-10']"
                        )

                        if label_elem is not None and value_elem is not None:
                            label = label_elem.text_content().strip().replace("\n", " ")
                            value = value_elem.text_content().strip().replace("\n", " ")
                            # Handle line breaks (`<br>`) in the value
                            value = " | ".join(value_elem.itertext()).strip()
                            data[label] = value

                            # Output or process the extracted data
                    print(f"Extracted Data for {href}:\n{data}\n")
