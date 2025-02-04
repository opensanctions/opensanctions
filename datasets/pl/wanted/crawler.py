import re
from typing import List

import countrynames

from zavod import Context
from zavod import helpers as h

NULL_CITIZENSHIPS = ["NIEUSTALONE", "BEZPAŃSTWOWIEC"]


def clean_and_split_citizenship(citizenship: str) -> str | List[str]:
    """
    Try to split the space-delimited citizenship field in various creative ways

    Args:
        citizenship: The string content of the citizenship field

    Returns:
        A string or a list of string containing the cleaned citizenship(s).

    """
    # Sometimes, we have another spelling of the country in brackets, like "USA (STANY ZJEDNOCZONE AMERYKI)"
    alt_country_match = re.search(r"\((?P<alt_country>.+)\)", citizenship)
    if alt_country_match:
        cleaned_citizenship = re.sub(r" \(.+\)", "", citizenship)
    else:
        cleaned_citizenship = citizenship

    if countrynames.to_code(cleaned_citizenship, fuzzy=True):
        return cleaned_citizenship

    if " " in cleaned_citizenship:
        split_citizenships = cleaned_citizenship.split(" ")
        # Somtimes a person is "NIEUSTALONE POLSKA" aka unknown polish
        split_citizenships = [
            c for c in split_citizenships if c not in NULL_CITIZENSHIPS
        ]

        # Multiple citizenships are space-delimited (yay), see if space-splitting them works
        if all([countrynames.to_code(x) for x in split_citizenships]):
            return split_citizenships
        else:
            # Sometimes, the citizenship looks like this "WIELKA BRYTANIA IZRAEL"
            # Covering this special case here (just one split) is easy and cheap
            for i in range(1, len(split_citizenships) + 1):
                citizenship_1 = " ".join(split_citizenships[:i])
                citizenship_2 = " ".join(split_citizenships[i:])
                if countrynames.to_code(
                    citizenship_1, fuzzy=True
                ) and countrynames.to_code(citizenship_2, fuzzy=True):
                    return [citizenship_1, citizenship_2]

    # Sometimes, there is a full spelling of the country in parens
    if alt_country_match:
        alt_country = alt_country_match.groupdict().get("alt_country")
        if countrynames.to_code(alt_country, fuzzy=True):
            return alt_country

    # If nothing worked, we just return the original value
    return citizenship


def crawl_person(context: Context, url: str):
    context.log.debug(f"Crawling person page {url}")

    person = context.make("Person")
    # TODO(Leon Handreke): Do we use the r12323456 in the URL that looks like a record ID?

    # There is an ID prefixed by r in the URL, which seems to refer to the record. The name component in the URL
    # can be changed at will.
    # URLs look like this: https://poszukiwani.policja.gov.pl/pos/form/r922391307881,OCHNIO-KRZYSZTOF.html
    url_match = re.search(r"/pos/form/(?P<rid>r\d+),.+.html", url)
    assert (
        url_match and "rid" in url_match.groupdict()
    ), "Could not extract r-ID from malformed person URL"
    rid = url_match.groupdict()["rid"]
    person.id = context.make_slug(rid)

    doc = context.fetch_html(url, cache_days=7)
    # Extract details using XPath based on the provided HTML structure
    details = {
        "full_name": "//div[@class='head']/h2/text()",
        "middle_name": "//p[contains(text(), 'Data urodzenia:')]/strong/text()",
        "father_name": "//p[contains(text(), 'Imię ojca:')]/strong/text()",
        "mother_name": "//p[contains(text(), 'Imię matki:')]/strong/text()",
        "mother_maiden_name": "//p[contains(text(), 'Nazwisko panieńskie matki:')]/strong/text()",
        "gender": "//p[contains(text(), 'Płeć:')]/strong/text()",
        "place_of_birth": "//p[contains(text(), 'Miejsce urodzenia:')]/strong/text()",
        "date_of_birth": "//p[contains(text(), 'Data urodzenia:')]/strong/text()",
        "alias": "//p[contains(text(), 'Pseudonim:')]/strong/text()",
        "citizenship": "//p[contains(text(), 'Obywatelstwo:')]/strong/text()",
        "height": "//p[contains(text(), 'Wzrost:')]/strong/text()",
        "eye_color": "//p[contains(text(), 'Kolor oczu:')]/strong/text()",
    }
    info = dict()
    for key, xpath in details.items():
        q = doc.xpath(xpath)
        if q:
            text = q[0].strip()
            if text != "-":
                info[key] = text

    person.add("sourceUrl", url)
    person.add("topics", "crime")
    person.add("topics", "wanted")
    # TODO(Leon Handreke): What does country mean in that context? The za_wanted crawler also does this.
    person.add("country", "pl")

    h.apply_name(person, full=info["full_name"], middle_name=info.get("middle_name"))
    h.apply_date(person, "birthDate", info.get("date_of_birth"))
    person.add("birthPlace", info.get("place_of_birth"))
    person.add("gender", info.get("gender"))
    person.add("alias", info.get("alias"))
    person.add("fatherName", info.get("father_name"))
    person.add("motherName", info.get("mother_name"))
    # TODO(Leon Handreke): Add mother maiden name or is that too detailed?
    person.add("motherName", info.get("mother_maiden_name"))
    person.add("height", info.get("height"))
    person.add("eyeColor", info.get("eye_color"))

    citizenship = info.get("citizenship")
    cleaned_citizenship = (
        clean_and_split_citizenship(citizenship) if citizenship else None
    )

    person.add("citizenship", cleaned_citizenship, original_value=citizenship)

    crimes = doc.xpath(
        "//p[contains(text(), 'Podstawy poszukiwań:')]/following-sibling::ul//a/text()"
    )
    if not crimes:
        context.log.warn("No crimes found for person", entity_id=person.id, url=url)
    # TODO(Leon Handreke): Every wanted list puts this in notes -- could there be a better place?
    person.add("notes", crimes)

    context.emit(person, target=True)


def crawl_index(context, url) -> str | None:
    context.log.info(f"Crawling index page {url}")
    doc = context.fetch_html(url, cache_days=1)
    # makes it easier to extract dedicated details page
    doc.make_links_absolute(context.dataset.data.url)
    cells = doc.xpath("//li[.//a[contains(@href, '/pos/form/r')]]/a/@href")
    for cell in cells:
        crawl_person(context, cell)

    # On the last page, the next button will not have an <a>, so this will not match
    next_button_href = doc.xpath(
        "//li/a/span[contains(text(), 'następna')]/parent::a/@href"
    )
    return next_button_href[0] if next_button_href else None


def crawl(context):
    next_url = context.dataset.data.url
    # Use this construction instead of recursion because Python sets a recursion limit
    while next_url:
        next_url = crawl_index(context, next_url)
