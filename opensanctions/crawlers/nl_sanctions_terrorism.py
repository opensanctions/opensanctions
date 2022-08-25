from lxml import etree
from zipfile import ZipFile
from zavod.parse.xml import remove_namespace
from datetime import datetime
from opensanctions import helpers as h

COLNAMESMAPPING={
    # columnheaders : ftm properties
    'Surname':'lastName',
    'First_name': "firstName", 
    'Alias': "name", 
    'DoB':"birthDate", # 31-01-2000
    'PoB': "birthPlace",
    "Decisiondate": "",
    "Link": ""
    }
DATEFORMAT = "%d-%m-%Y"
def crawl(context):
    # this file is ods (open document sheet)
    # ods is a bunch of xmls in a zip
    source_path = context.fetch_resource('eng-terrorismelijst.ods', context.dataset.data.url)
    with ZipFile(source_path, "r") as zip:
        for name in zip.namelist():
            # content.xml contains the actual values.
            if name == "content.xml":
                with zip.open(name) as fh:
                    doc = etree.parse(fh)
                    doc = remove_namespace(doc)
                    root=doc.getroot()
                    all_tablerows=root.findall('.//table-row')
                    # I know this data starts at the third row (headings)
                    #headings=all_tablerows[2].findall('.//p')
                    #columnheaders=[col.text for col in headings]
                    # TODO: assert that right columns here

                    for row in all_tablerows[3:]:
                        # go through the cols in a row
                        person = context.make("Person")
                        person.add("topics", "crime")
                        
                        for i, cell in enumerate(row.getchildren()[0:7]):
                            context.log.debug(f"in cell {i}")
                            colname=list(COLNAMESMAPPING.keys())[i]
                            # make sure the cel contains data
                            if len(cell.getchildren()) >0:
                                # is it sanction information?
                                if colname =='Link':         
                                    url=cell.getchildren()[0].getchildren()[0].values()[0]                           
                                    name_publication=cell.getchildren()[0].getchildren()[0].text
                                    
                                elif colname=='Decisiondate':
                                    datestring=cell.getchildren()[0].text
                                    decisiondate=datetime.strptime(datestring, DATEFORMAT)
                                else:
                                    # it is person information
                                    value_=cell.getchildren()[0].text
                                    if colname == 'DoB':
                                        datestring=cell.getchildren()[0].text
                                        value_=datetime.strptime(datestring, DATEFORMAT)
                                    context.log.debug(f"{value_}, {colname}")
                                    person.add(COLNAMESMAPPING[colname], value_)

                        sanction = context.make("Sanction")
                        sanction.id = context.make_id("Sanction", entity.id, key)
                        sanction.add("entity", person)
                        if dataset.publisher.country != "zz":
                            sanction.add("country", dataset.publisher.country)
                        sanction.add("authority", dataset.publisher.name)
                        sanction.add("sourceUrl", dataset.url)
                        sanction.add("program", "National sanctionlist terrorism (NL)") # program (label)
                        sanction.add("provisions", "Freezing of assets") # Scope of sanctions (label)
                        sanction.add("reason", "Individual or organisation involved in terrorist activities.") # reason, text
                        sanction.add("listingDate", decisiondate)
                        sanction.add("sourceUrl", url)
                                
                        # emit person and sanction

                        context.log.debug(f"created entity {person.to_dict()}")
                        context.log.debug(f"created sanction {sanction.to_dict()}")
                        context.emit(person, target=True)
                        context.emit(sanction)
                        

def parse_row(row) :
    # build dictionary
    dictionary={}
    for i, cell in enumerate(row.getchildren()[0:7]):
        colname=list(COLNAMESMAPPING.keys())[i]
        print(cell.getchildren())
        # third one does not have text

        dictionary[colname] = cell.getchildren()[0].text
    return dictionary

    # with open(source_path, 'r') as fh:
    #     print(len(fh.read()))
    

    # columns
    # starts at row 3 with columntitles
    # Surname
    # First name(s)
    # Alias
    # Date of Birth (DD-MM-JJJJ)
    # Place of Birth
    # Date of ministerial decision (DD-MM-JJJJ)
    # Link offical notification
    # link to offical notification