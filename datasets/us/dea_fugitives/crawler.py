from zavod import Context, helpers as h

def parse_table(table):
    """This function is used to parse the table of Labels and Descriptions
    about the fugitive and return as a dict.
    """

    info_dict = {}

    # The first row will always be the header (Label, Description)
    # So we can skip it.
    for row in table.findall(".//tr")[1:]:
        label = row.findtext(".//td[1]")
        description = row.findtext(".//td[2]")
        info_dict[label] = description
    
    return info_dict


def crawl_item(fugitive_url: str, context: Context):

    response = context.fetch_html(fugitive_url)

    name = response.findtext('.//h2[@class="fugitive__title"]')
    info_dict = parse_table(response.find('.//table'))

    entity = context.make("Person")
    entity.id = context.make_id(name)

    if "Sex" in info_dict:
        if info_dict["Sex"] == "Male":
            entity.add("gender", "male")
        else:
            entity.add("gender", "female")

    if "Year of Birth" in info_dict:
        entity.add("birthDate", info_dict["Year of Birth"])


    sanction_description = ''.join([d.text_content() for d in response.findall('.//div[@class="meta"]') if "Wanted for the following" in d.text_content()])

    if "NCIC #" in info_dict:
        sanction = h.make_sanction(context, entity, key=info_dict["NCIC #"])
    else:
        sanction = h.make_sanction(context, entity)

    sanction.add("description", sanction_description)
    sanction.add("sourceUrl", fugitive_url)

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    # Each page only displays 10 fugitives at a time, so we need to loop until we don't find any more fugitives

    base_url = context.data_url

    page_num = 0

    while True:
        url = base_url + "?page=" + str(page_num)
        response = context.fetch_html(url, cache_days=1)
        response.make_links_absolute(url)

        for item in response.findall('.//div[@class="teaser "]/div/h3/a'):
            crawl_item(item.get('href'), context)

        page_num += 1