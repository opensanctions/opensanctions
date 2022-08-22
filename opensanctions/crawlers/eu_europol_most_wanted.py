import re
from datetime import datetime
from unicodedata import normalize

from lxml import html


def crawl(context):
    base_url = context.dataset.data.url  # "https://eumostwanted.eu/"
    response = context.http.get(base_url)
    doc = html.fromstring(response.text)
    # Find all link on page
    all_links = doc.findall(".//a")
    # remove all other links
    most_wanted = [
        a.get("href") for a in all_links if a.get("href", "").startswith("http")
    ]
    # filter out other links so we have only links to most_wanted peoples
    most_wanted_cleaned = [link.replace(base_url, "") for link in most_wanted]
    most_wanted_cleaned = [
        link
        for link in most_wanted_cleaned
        if link not in ("legal-notice", "https://www.europol.europa.eu")
    ]
    context.log.info(f"Found {len(most_wanted_cleaned)} people on the eu-most-wanted list")
    # crawl every pages and create a new person
    for person_id in most_wanted_cleaned:
        context.log.info(f"crawling {person_id}")
        crawl_person(context, person_id, base_url)


def crawl_person(context,person_id, base_url):
    """read and parse every person-page"""
    new_url = base_url + person_id
    p_response = context.http.get(new_url)
    p_doc = html.fromstring(p_response.text)
    xpath_infofield = '//*[contains(concat( " ", @class, " " ), concat( " ", "wanted_top_right", " " ))]'
    infofields = p_doc.xpath(xpath_infofield)[0]
    # use values and keys() to filter, there is propably a better way to do
    # this but at least it workeds.
    person = context.make("Person")
    person.id = context.make_slug(person_id)
    person.add("topics", "crime")
    for field in infofields.getchildren():
        # some of the fields don't have values, so skipped them
        if len(field.values())>0:
            if "field-name-title-field" in field.values()[0]:
                name_ = field.getchildren()[0].text
                context.log.debug(f"found name {name_}")
                person.add("name", name_)
                
                if name_.rfind(',') > 0:
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
                alias_ = normalize("NFKD", field.text_content())
                alias_ = alias_.replace("Alias:", "").strip()
                context.log.debug(f"adding alias {alias_} to name")
                # split on [-,;]
                split_string = re.split(r',|-|;',alias_)
                for aliasstring in split_string:
                    # remove ' and "
                    aliasstring=re.sub("[\"\']", "", aliasstring)
                    person.add("name", aliasstring.strip())
            if "field-name-field-gender" in field.values()[0]:
                gender_ = field.getchildren()[1].text_content()
                context.log.debug(f"adding gender {gender_} to gender")
                person.add("gender", gender_)
            if "field-name-field-date-of-birth" in field.values()[0]:
                dob_ = field.getchildren()[1].text
                context.log.debug(f"adding dob {dob_} to birthDate")
                person.add('birthDate', parse_date(dob_))
            if "field-name-field-nationality" in field.values()[0]:
                nationality_ = field.getchildren()[1].getchildren()[0].text
                context.log.debug(f"adding nationality {nationality_} to nationality")
                person.add("nationality", nationality_)
   
    context.log.debug(f"created entity {person.to_dict()}")
    context.emit(person, target=True)

def parse_date(datestring):
    # examples
    #time1="Mar 22, 1983"
    #time2="Apr 25, 1974"
    # this is format "%b %d, %Y"
    format="%b %d, %Y"
    return datetime.strptime(datestring,format)