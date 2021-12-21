# Query: https://w.wiki/4Z73
PROPS_FAMILY = {
    "P7": "sibling",
    "P9": "sibling",
    "P22": "parent",
    "P26": "spouse",
    "P25": "parent",
    "P40": "child",
    "P43": "stepparent",
    "P44": "stepparent",
    "P451": "unmarried partner",
    "P1038": "relative",
    "P1290": "godparent",
    "P3373": "sibling",
    "P3448": "stepparent",
    "P8810": "unspecified parent",
}

PROPS_ASSOCIATION = {
    "P1327": "partner in business or sport",
    "P3342": "significant person",
}

# https://www.wikidata.org/wiki/Wikidata:List_of_properties/human
PROPS_DIRECT = {
    "P1477": "alias",  # birth name
    "P1813": "alias",  # short name
    "P2561": "alias",  # name
    "P1559": "alias",  # name in native language
    "P2562": "alias",  # married name
    "P511": "title",
    "P735": "firstName",
    "P734": "lastName",
    "P1950": "lastName",
    "P21": "gender",
    "P39": "position",
    "P140": "religion",
    "P569": "birthDate",
    "P5056": "fatherName",
    "P570": "deathDate",
    "P19": "birthPlace",
    "P856": "website",
    "P512": "education",
    "P69": "education",
    "P27": "nationality",
    "P742": "weakAlias",
    "P172": "ethnicity",
    "P973": "sourceUrl",
    "P17": "country",
    "P571": "incorporationDate",
    "P1454": "legalForm",
}

PROPS_QUALIFIED = (
    "position",
    "education",
)


IGNORE = set(
    [
        "P18",  # image
        "P109",  # signature
        "P166",  # award received
        "P793",  # significant event
        "P1344",  # participant in
        "P950",  # Biblioteca Nacional de España ID
        "P9629",  # Armeniapedia ID
        "P949",  # National Library of Israel ID
        "P9368",  # CNA topic ID
        "P935",  # Commons gallery
        "P9037",  # BHCL UUID
        "P5019",  # Brockhaus Enzyklopädie online ID
        "P4619",  # National Library of Brazil ID
        "P3509",  # Dagens Nyheter topic ID
        "P3106",  # Guardian topic ID
        "P268",  # Bibliothèque nationale de France ID
        "P244",  # Library of Congress authority ID
        "P227",  # GND ID
        "P214",  # VIAF ID
        "P213",  # ISNI
        "P1816",  # National Portrait Gallery (London) person ID
        "P1368",  # LNB ID
        "P1284",  # Munzinger person ID
        "P8687",  # social media followers
        "P8179",  # Canadiana Name Authority ID
        "P8094",  # GeneaStar person ID
        "P7982",  # Hrvatska enciklopedija ID
        "P866",  # Perlentaucher ID
        "P8850",  # CONOR.KS ID
        "P7859",  # WorldCat Identities ID
        "P7929",  # Roglo person ID
        "P7293",  # PLWABN ID
        "P7666",  # Visuotinė lietuvių enciklopedija ID
        "P648",  # Open Library ID
        "P6200",  # BBC News topic ID
        "P5361",  # BNB person ID
        "P4638",  # The Peerage person ID
        "P3987",  # SHARE Catalogue author ID
        "P345",  # IMDb ID
        "P3417",  # Quora topic ID
        "P3365",  # Treccani ID
        "P2924",  # Great Russian Encyclopedia Online ID
        "P2163",  # FAST ID
        "P1695",  # NLP ID (unique)
        "P1263",  # NNDB people ID
        "P1207",  # NUKAT ID
        "P109",  # signature
        "P1005",  # Portuguese National Library ID
        "P1006",  # Nationale Thesaurus voor Auteurs ID
        "P1015",  # NORAF ID
        "P646",  # Freebase ID
    ]
)
