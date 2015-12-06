from lxml import etree
from datetime import datetime

from pepparser.util import remove_namespace, make_id


PUBLISHER = {
    'publisher': 'US Treasury Office of Foreign Assets Control',
    'publisher_url': 'https://www.treasury.gov/about/organizational-structure/offices/Pages/Office-of-Foreign-Assets-Control.aspx',    
}


def ofac_parse(emit, sdn, consolidated, xmlfile):
    doc = etree.parse(xmlfile)
    remove_namespace(doc, 'http://tempuri.org/sdnList.xsd')

    publish_date = datetime.strptime(doc.findtext('.//Publish_Date'),
                                     '%m/%d/%Y')
    source = {
        'url': 'http://sdnsearch.ofac.treas.gov/'
    }
    source.update(PUBLISHER)
    if sdn:
        source['title'] = 'Specially Designated Nationals and Blocked Persons'
    if consolidated:
        source['title'] = 'Consolidated non-SDN List'

    for entry in doc.findall('.//sdnEntry'):
        uid = entry.findtext('uid')
        # record_url = 'https://sdnsearch.ofac.treas.gov/Details.aspx?id=%s' % uid
        # print record_url
        record = {
            'id': make_id('us', 'ofac', uid),
            'updated_at': publish_date.date(),
            'source': source
        }
        emit.entity(record)
