import re
from normality.cleaning import remove_unsafe_chars

from opensanctions import helpers as h


def parse_date(datestring):
    return h.parse_date(datestring, ["%b %d, %Y"])


def crawl(context):
    base_url = context.dataset.data.url
    doc = context.fetch_html(base_url)
    for link in doc.findall(".//a"):
        url = link.get("href")
        if url is None or not url.startswith("https:"):
            continue
        if "legal-notice" in url:
            continue
        person_id = url.replace(base_url, "")
        if person_id == url:
            continue
        crawl_person(context, person_id, url)


def crawl_person(context, person_id, url):
    """read and parse every person-page"""
    doc = context.fetch_html(url)
    xpath_infofield = '//*[contains(concat( " ", @class, " " ), concat( " ", "wanted_top_right", " " ))]'
    infofields = doc.xpath(xpath_infofield)[0]
    # use values and keys() to filter, there is propably a better way to do
    # this but at least it workeds.
    person = context.make("Person")
    person.id = context.make_slug(person_id)
    person.add("topics", "crime")
    person.add("sourceUrl", url)
    for field in infofields.getchildren():
        # some of the fields don't have values, so skipped them
        if len(field.values()) > 0:
            if "field-name-title-field" in field.values()[0]:
                name_ = field.getchildren()[0].text
                context.log.debug(f"found name {name_}")
                person.add("name", name_)

                if name_.rfind(",") > 0:
                    # if no , it will return -1 and
                    # therefore skip firstname/lastname part.
                    firstname_ = name_.split(",")[1].strip().title()
                    lastname_ = name_.split(",")[0].strip().title()
                    if firstname_ is not None:
                        person.add("firstName", firstname_)
                    if lastname_ is not None:
                        person.add("lastName", lastname_)
            if "field-name-field-alias" in field.values()[0]:
                # there are weird \xa characters in the field.
                alias_ = remove_unsafe_chars(field.text_content())
                alias_ = alias_.replace("Alias:", "").strip()
                context.log.debug(f"adding alias {alias_} to name")
                # split on [-,;]
                split_string = re.split(r",|-|;", alias_)
                for aliasstring in split_string:
                    # remove ' and "
                    aliasstring = re.sub("[\"']", "", aliasstring)
                    person.add("alias", aliasstring.strip())
            if "field-name-field-gender" in field.values()[0]:
                gender_ = field.getchildren()[1].text_content()
                context.log.debug(f"adding gender {gender_} to gender")
                person.add("gender", gender_)
            if "field-name-field-date-of-birth" in field.values()[0]:
                dob_ = field.getchildren()[1].text
                context.log.debug(f"adding dob {dob_} to birthDate")
                person.add("birthDate", parse_date(dob_))
            if "field-name-field-nationality" in field.values()[0]:
                nationality_ = field.getchildren()[1].getchildren()[0].text
                context.log.debug(f"adding nationality {nationality_} to nationality")
                person.add("nationality", nationality_)

    context.emit(person, target=True)
