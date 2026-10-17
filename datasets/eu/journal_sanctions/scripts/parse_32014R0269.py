"""Parse consolidated Regulation (EU) 269/2014 (Ukraine) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list, the EU's largest: parts
  "Persons" and "Entities" (no part letters; numbering restarts per part,
  so the annex identifiers are I.PERSONS and I.ENTITIES), each one
  five-column table (entry number, name, identifying information, reasons,
  date of listing). Travel bans live in Decision 2014/145/CFSP.
- Annex II — competent-authority websites, not designations.

Name cells print the Latin name on the first line — kept whole, including
inline parentheticals and a.k.a. tails, per the contract's unlabeled-
combined-form rule — and further renderings one per line: parenthesized
native-script or annotated lines, bare Latin or Cyrillic transliteration
variants, and labelled a.k.a. lists. Every such further line is an alias
(split on ";" only); a parenthesized rendering may wrap across lines and
is joined until its closing parenthesis; "Formerly known as …" lines are
previousName. Entry numbers may carry a lowercase insertion suffix
("174a."). A handful of rows continue the previous entry (empty number
cell, or the two pinned four-cell rows missing the number column, or one
pinned single-cell reasons row).

Identifying-information cells are label-driven with a large vocabulary.
Entity entries printing a KPP-family or IMO-family label are emitted as
Company (kppCode and imoNumber exist only there). Labels with no contract
column are deliberately dropped and documented: relational lines naming
other parties (Associated …, Parent, Owner, Liquidator, General Director,
CEO, Subsidiaries), identity-document attributes (Date of issue/expiry,
Issued by), Russian statistical codes (OKFS, OKOGU, OKOPF, FSFR, OKATO),
and identifier systems the contract cannot hold (blockchain wallets and
commercial data-vendor IDs). Dates are transcribed as the source prints
them ("21.7.2014"); the crawler normalizes dates.

Output: data/consolidated/32014R0269.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `config.consolidation`, updated in the same commit as the CSV.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import click
from .common import (
    LABELLED_RE,
    SKIP_P_CLASSES,
    ParseError,
    Row,
    annex_blocks,
    cell_lines,
    check_consolidated_celex,
    check_marker,
    check_registry,
    clean,
    load_source,
    split_values,
    summary,
    to_record,
    validate_records,
    verbatim_date,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.util import Element

FRAMEWORK_CELEX = "32014R0269"
PROGRAM_KEY = "EU-UKR"
# Annex I implements the regulation's Article 2 fund freeze; travel bans
# live in Decision 2014/145/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})
SUBTITLE = (
    "List of natural and legal persons, entities and bodies referred to in Article 2"
)
# (part heading, part id, default schema) in print order; numbering
# restarts per part, so the part name joins the annex identifier.
PARTS = (
    ("Persons", "PERSONS", "Person"),
    ("Entities", "ENTITIES", "LegalEntity"),
)
HEADER = ("", "Name", "Identifying information", "Reasons", "Date of listing")

# Entry numbers, including corrigendum-inserted lowercase suffixes ("174a.").
NUMBER_RE = re.compile(r"^(\d+)([a-z]?)\.$")
# a.k.a./alias label prefixes as printed: "a.k.a.", "a.k.a", "A.k.a.",
# "AKA", "Alias:", "alias ", optionally glued to the value ("a.k.a.ECOOIL").
AKA_RE = re.compile(r"^(?:a\.k\.a\.?:? ?|A\.k\.a\.:? ?|AKA:? ?|[Aa]lias:? )(.+)$")
BARE_AKA_RE = re.compile(r"^(?:a\.k\.a\.?|A\.k\.a\.|AKA|[Aa]lias):?$")
# Leading and mid-value a.k.a. labels inside one printed rendering: the
# label is structure, never part of the alias value, and a repeated label
# marks a further alias (Myanmar precedent).
AKA_STRIP_RE = re.compile(r"^(?:a\.k\.a\.?|A\.k\.a\.|AKA|[Aa]lias):? ?")
AKA_SPLIT_RE = re.compile(r" a\.k\.a\.?:? ")
# Bare language-annotation lines label the following or same-line rendering
# ("Russian: Общество …", "Arabic:").
LANG_LINE_RE = re.compile(r"^(?:Russian|Ukrainian|Belarusian|Arabic): ?(.*)$")
# "Formerly known as …" renderings, parenthesized or bare → previousName.
FKA_RE = re.compile(r"^\(?[Ff]ormerly known as[:,]? (.+?)\)?$")
# Fully parenthesized rendering; groups may wrap across lines until the
# closing parenthesis (a trailing list comma/semicolon after it is print
# punctuation and stripped).
PAREN_LINE_RE = re.compile(r"^\((.+)\)[,;]?$")
GROUP_CLOSE_RE = re.compile(r"^(.*)\)[,;]?$")
# Language-annotation prefixes inside parenthesized renderings are printed
# labels ("Russian: …"), stripped so the alias holds the rendering only.
LANG_PREFIX_RE = re.compile(
    r"^(?:Russian|Ukrainian|Belarusian|Uzbek|Serbian|Croatian)(?: spelling)?: ?(.+)$"
)

# Identifying-information labels → CSV column, exactly as printed
# (whitespace-stripped). Assembled from the profiled vocabulary of the
# 20260717 consolidated text; an unknown label is a review event.
INFO_LABELS = {
    # birth
    "DOB": "birthDate",
    "D.O.B": "birthDate",
    "Date of birth": "birthDate",
    "Possible DOB": "birthDate",
    "POB": "birthPlace",
    # Entry 2111, added by M125, prints the dotted form alongside "D.O.B".
    "P.O.B": "birthPlace",
    "Place of birth": "birthPlace",
    # person basics
    "Gender": "gender",
    "Nationality": "nationality",
    "Nationalities": "nationality",
    # roles
    "Function": "position",
    "Functions": "position",
    "Position": "position",
    "Position(s)": "position",
    "Rank": "position",
    "Military rank": "position",
    "Profession": "position",
    # passports
    "Passport": "passportNumber",
    "Passport number": "passportNumber",
    "Passport numbers": "passportNumber",
    "Passport No": "passportNumber",
    "International passport number": "passportNumber",
    "Israeli passport number": "passportNumber",
    "Israel passport number": "passportNumber",
    "Russian passport numbers": "passportNumber",
    # generic identity documents
    "ID number": "idNumber",
    "ID Number": "idNumber",
    "National ID": "idNumber",
    "National ID number": "idNumber",
    "National ID no": "idNumber",
    "Personal ID": "idNumber",
    "Identity document": "idNumber",
    "Identity document number": "idNumber",
    "Passport or ID number": "idNumber",
    "Passport/ID numbers": "idNumber",
    # Russian personal insurance account number, printed for entry 2113 (M125).
    "SNILS": "idNumber",
    "Entity-based ID": "idNumber",
    "Unique Entity Identifier (SAM)": "idNumber",
    # tax identifiers (INN-explicit forms → innCode)
    "INN": "innCode",
    "INN/TIN": "innCode",
    "Tax ID (INN no.)": "innCode",
    "Tax Identification Number (INN)": "innCode",
    "Tax Identification Number (ИНН)": "innCode",
    "Taxpayer Identification number (INN)": "innCode",
    "Russian taxpayer number (INN)": "innCode",
    "TIN": "taxNumber",
    "Tax number": "taxNumber",
    "Tax Number": "taxNumber",
    "Tax ID": "taxNumber",
    "Tax ID No.": "taxNumber",
    "Tax ID No": "taxNumber",
    "Tax ID number": "taxNumber",
    "Tax payer ID": "taxNumber",
    "Tax Identification Number": "taxNumber",
    "Tax Identification Number (Ukraine)": "taxNumber",
    "Tax Identification number": "taxNumber",
    "Tax identification Number": "taxNumber",
    "Tax identification number": "taxNumber",
    "Tax Identificiation Number": "taxNumber",
    "Tax Indentification Number": "taxNumber",
    "Taxpayer Identification Number": "taxNumber",
    "Taxpayer Identification number": "taxNumber",
    "Taxpayer identification number": "taxNumber",
    "Individual tax number": "taxNumber",
    "Individual Tax Number": "taxNumber",
    "Tax Individual Number": "taxNumber",
    "Tax individual number": "taxNumber",
    "National Tax ID": "taxNumber",
    "Company Tax Identification Number": "taxNumber",
    "Tax registration number": "taxNumber",
    "Tax Registration Number": "taxNumber",
    "VAT": "taxNumber",
    "VAT ID": "taxNumber",
    "VAT number": "taxNumber",
    "UNP": "taxNumber",
    # Russian company codes
    "KPP": "kppCode",
    "— KPP": "kppCode",
    "PPC": "kppCode",
    "Registration reason code": "kppCode",
    "OGRN": "ogrnCode",
    "— OGRN": "ogrnCode",
    "OGRN number": "ogrnCode",
    "PSRN": "ogrnCode",
    "Primary State registration number": "ogrnCode",
    "Primary state registration number (OGRN)": "ogrnCode",
    "Registration ID (OGRN no.)": "ogrnCode",
    "Registration number (OGRN)": "ogrnCode",
    "ОГРН/main state registration number": "ogrnCode",
    "Main state registration number": "ogrnCode",
    # The sole-trader register's number (15 digits), printed for the natural
    # person of entry 2116 (M125); it is an OGRN of the individual-entrepreneur
    # series, which is what `ogrnCode` holds.
    "OGRNIP": "ogrnCode",
    "OKPO": "okpoCode",
    "— OKPO": "okpoCode",
    # registration numbers
    "Reg. number": "registrationNumber",
    "Registration number": "registrationNumber",
    "Registration Number": "registrationNumber",
    "Registration No.": "registrationNumber",
    "Reigstration number": "registrationNumber",
    "Registration numbers": "registrationNumber",
    "Registration ID": "registrationNumber",
    "State registration number": "registrationNumber",
    "State Registration Number": "registrationNumber",
    "Business registration number": "registrationNumber",
    "Buisness registration number": "registrationNumber",
    "Company registration number": "registrationNumber",
    "Trade register number": "registrationNumber",
    "Economic register number": "registrationNumber",
    "Economic Register Number (CBLS)": "registrationNumber",
    "CBLS": "registrationNumber",
    "Local licence number": "registrationNumber",
    "Licence number": "registrationNumber",
    "Licence Number": "registrationNumber",
    "Business Licence No.": "registrationNumber",
    "Import and export enterprise code": "registrationNumber",
    "China Company Registration Number": "registrationNumber",
    "Native Belize company number": "registrationNumber",
    "Corporate Identification Number (CIN)": "registrationNumber",
    "Corportate Identification Number": "registrationNumber",
    "Unified social credit code": "registrationNumber",
    "Unified Social Credit Code": "registrationNumber",
    "China Unified Social Credit Code": "registrationNumber",
    "BIN/OGRN": "registrationNumber",
    # maritime, banking and market identifiers
    "IMO": "imoNumber",
    "IMO number": "imoNumber",
    "IMO Number": "imoNumber",
    "SWIFT/BIC": "swiftBic",
    "LEI": "leiCode",
    "Legal Entity Identifier": "leiCode",
    "Stock code (Shenzhen Stock Exchange)": "ticker",
    # incorporation
    "Date of registration": "incorporationDate",
    "Date of Registration": "incorporationDate",
    "Registration date": "incorporationDate",
    "Registration Date": "incorporationDate",
    "Date of initial registration": "incorporationDate",
    "Date of creation": "incorporationDate",
    "Date of incorporation": "incorporationDate",
    "Founded": "incorporationDate",
    # entity form and name
    "Type of entity": "legalForm",
    "Type of entitiy": "legalForm",
    "Type of company": "legalForm",
    "Full corporate name": "alias",
    # addresses
    "Address": "address",
    "Addresses": "address",
    "Adress": "address",
    "Address (Work)": "address",
    "Address (HQ)": "address",
    "Address of Headquarters": "address",
    "Headquarters": "address",
    "Legal address": "address",
    "Legal Address": "address",
    "Former address": "address",
    "Former legal address": "address",
    "Postal address": "address",
    "Postal Address": "address",
    "Correspondence address": "address",
    "Contact address": "address",
    "Mailing address": "address",
    "Second address": "address",
    "Other addresses": "address",
    "Temporary address": "address",
    "Moscow office": "address",
    "Offices": "address",
    "Location": "address",
    "Location of activities": "address",
    "Suspected location": "address",
    # contact
    "Telephone": "phone",
    "Telephone.": "phone",
    "Telephones": "phone",
    "Tel.": "phone",
    "Tel": "phone",
    # Printed with a Cyrillic "Т" (U+0422) on one entry.
    "Тel": "phone",
    "Tel./fax": "phone",
    "Tel./Fax": "phone",
    "Phone": "phone",
    "Phone number": "phone",
    "Phone and fax": "phone",
    "Fax": "phone",
    "Telephone volunteers in Russia": "phone",
    "Email": "email",
    "email": "email",
    "e-mail": "email",
    "E-mail": "email",
    "Еmail": "email",
    "EMail": "email",
    "Mail": "email",
    "E-mail address": "email",
    "e-mail address": "email",
    "Website": "website",
    "Websites": "website",
    "Web": "website",
    "Webpage": "website",
    "Telegram": "website",
    "Telegram channel": "website",
    "Social media": "website",
    "Social network profile": "website",
    "Media resources": "website",
}
# Free-text labels whose prose value goes to `notes`, label stripped. Their
# values frequently smuggle labelled identifiers; each value is re-routed
# through the line rules first and only prose reaches notes.
NOTES_LABELS = frozenset(
    {
        "Other information",
        "Other identifying information",
        "Additional information",
        "Official information",
        "Social media and other information",
    }
)
# Labels with no CSV column, deliberately not transcribed. Relational lines
# name other parties; Date of issue/expiry and Issued by qualify the
# identity document printed above them; the dash-prefixed Russian
# statistical codes, blockchain wallets and commercial data-vendor
# identifiers have no contract columns. The label line and its bare
# continuation lines are consumed.
DROP_LABELS = frozenset(
    {
        "Associated entities",
        "Associated entites",
        "Associated entity",
        "Associated individuals",
        "Associated individual",
        "Associated individuals or entities",
        "Associated entities and individuals",
        "Associated persons",
        "Associated person",
        "Associates",
        "Other associated entities",
        "Parent",
        "Parent company",
        "Owner",
        "Liquidator",
        "General Director",
        "CEO",
        "Subsidiaries",
        "Date of issue",
        "Date of expiry",
        "Expiry Date",
        "— OKFS",
        "— OKOGU",
        "— OKOPF",
        "— FSFR",
        "— OKATO",
        "Known blockchain wallet addresses",
        "ETH",
        "BTC",
        "BSC",
        "TRX",
        "Acuris Unique ID",
        "Cedar Rose Internal Identifier",
        "EDI Global Issuer ID",
    }
)
# Labels the LABELLED_RE 40-char cap cannot match, checked as prefixes.
LONG_LABELS = {
    "Passport number, national ID number, other numbers of identity documents: ": "idNumber",
    "Address of the reception office in the Russian Federation: ": "address",
    "Telephone and fax numbers of the reception office in the Russian Federation: ": "phone",
    "Primary state identification number (OGRN): ": "ogrnCode",
    (
        "Date of registration in the Russian Federation (following the "
        "illegal referenda held in the occupied territories of Ukraine): "
    ): "incorporationDate",
}
# Colon-less labelled prefixes printed on bare lines.
COLONLESS_PREFIXES = (
    ("Tax ID No. ", "taxNumber"),
    ("Tax Identification Number ", "taxNumber"),
    ("Tax identification Number ", "taxNumber"),
    ("Tax ID (INN no.) ", "innCode"),
    ("TIN/INN ", "innCode"),
    ("POB ", "birthPlace"),
    ("Nationality ", "nationality"),
    ("Gender ", "gender"),
    ("KPP ", "kppCode"),
    ("КПП ", "kppCode"),
    ("INN ", "innCode"),
    ("OGRN ", "ogrnCode"),
    ("PSRN ", "ogrnCode"),
    ("OKPO ", "okpoCode"),
    ("PPC ", "kppCode"),
    ("IEC ", "registrationNumber"),
    ("BIN ", "registrationNumber"),
    ("Unified Social Credit Code ", "registrationNumber"),
    ("Registration number ", "registrationNumber"),
    ("Tel. ", "phone"),
    ("Address ", "address"),
    ("Founded in ", "incorporationDate"),
    ("Founded on ", "incorporationDate"),
)
# Registration labels in the pinned consolidation include countries, localities,
# full addresses, and one historical statement. Review any new printed value
# before deciding whether it belongs in jurisdiction or address.
REGISTRATION_LABELS = frozenset(
    {"Place of registration", "Place of Registration", "Place of registation"}
)
REGISTRATION_TERRITORIES = frozenset(
    {
        "Azerbaijan",
        "Belarus",
        "Belize",
        "Crimea",
        "DPRK",
        "Democratic People’s Republic of Korea",
        "Georgia",
        "Hong Kong",
        "India",
        "Israel",
        "Kazakhstan",
        "Kyrgyzstan",
        "Netherlands Antilles",
        "Oman",
        "People’s Republic of China",
        "Republic of Belarus",
        "Republic of Mauritius",
        "Russia",
        "Russian Federation",
        "Singapore",
        "Türkiye",
        "Ukraine",
        "United Arab Emirates",
        "United Kingdom",
        "United States of America",
        "Vietnam",
    }
)
REGISTRATION_REVIEWED_VALUES = frozenset(
    {
        "01, Yuzhny mkr, Iskitim, Novosibirsk region, 633208, Russian Federation",
        "03-32, Nordic European Centre 3, International Business Park Singapore, Republic of Singapore, 609927",
        "107078, Moscow, Masha Poryvaeva str., 34, Russian Federation",
        "107140, Moscow, Moskovsko-Kazansky Lane, 11-15, floor/room 1/7",
        "12 26-y Piket, Aldan, Aldanskiy Ulus, Republic of Sakha (Yakutia), 678900 (Russia)",
        "127254, Moscow, Ogorodny Prospekt, Building 5, Structure 4, Floor 3, Room 305",
        "127287, st. Khutorskaya 2nd, 38A, Moscow, Russian Federation",
        "129164, Moscow, Prospekt Mira, 126",
        "19 Vavilova St., 117997 Moscow, Russian Federation",
        "222511, Republic of Belarus, Minsk Region, Borisov District, Borisov, Gagarin St., 54",
        "27 Kalanchevskaya str. Moscow, 107078, Russian Federation",
        "300002, Russian Federation, Tula, Mosina str., 2",
        "412310, Saratov Region, Balashov, Ul. Titova, D.40",
        "423580, Nizhnekamsk, industrial zone, Republic of Tatarstan, Russia",
        "428903, Russian Federation, Chuvash Republic, Cheboksary, pr. I.Yakovleva, 4",
        "630004, Ulitsa Lenina d. 52, Novosibirsk, Russian Federation",
        "634041, Russian Federation, Tomsk region, Tomsk, Kirov avenue, Building 51D",
        "672530, Transbaikal territory, Chita district, 1 Atamanovskoye, Atamanovka, ul. Zavodskaya, d.1",
        "676811 Amurskaya oblast, Belogorskiy Rayon, Vozhaevka, Russian Federation",
        "Abu Dhabi, United Arab Emirates",
        "Alabuga, Russian Federation",
        "Azerbaijan",
        "Belarus",
        "Bishkek, Kyrgyzstan",
        "Bolshaya Polyanka str., 50/1 STR.1, 119180, Moscow, Russian Federation",
        "Brodnikov pereulok 7, Moscow, Russian Federation",
        "Bryansk region, Bryansk, Russian Federation",
        "Central Federal District of the Russian Federation",
        "Chelyabinsk Oblast, Russian Federation",
        "Crimea",
        "Crimea, Ukraine",
        "DPRK",
        "Degtyarnyy Pereulok, 11a, Saint Petersburg, 191144, Russian Federation",
        "Democratic People’s Republic of Korea",
        "Donetsk, Ukraine",
        "Dubai, United Arab Emirates",
        "Dubai, United Arab Emirates (Unit OT 17-32, Level 17, Central Park Offices, Dubai International Financial Centre, Dubai, PO Box 507065, United Arab Emirates)",
        "Ekatrinburg, Russian Federation",
        "Firstly Ukraine then, after the illegal annexation of Crimea, Russian Federation",
        "Georgia",
        "Hong Kong",
        "Hong Kong, People’s Republic of China",
        "India",
        "Isfahan, Iran",
        "Israel",
        "Izhevsk, Russian Federation",
        "Kaspiysk, Republic of Dagestan, Russian Federation",
        "Kazakhstan",
        "Kirov, Russian Federation",
        "Komsomolsky Pr. 22, Donetsk city, Donetsk District, so-called “Donetsk People’s Republic”",
        "Kuibyshev, Novosibirsk region, Russian Federation",
        "Kumertau, Russian Federation",
        "Kyrgyzstan",
        "Leningradskaya street, building 41, Hurzuf, Yalta, Crimea, Ukraine",
        "Magadan, Russia",
        "Minsk, Belarus",
        "Minsk, Republic of Belarus",
        "Moscow",
        "Moscow, Russia",
        "Moscow, Russian Federation",
        "Moscow, Russian Federation (107450, Moscow, Myasnitskaya st., 39/1)",
        "Moscow, Russian Federation (117525, Moscow, st. Dnepropetrovskaya, D. 3, K. 5)",
        "Moscow, Russian Federation (125080, Moscow, sh. Volokolamskoe, 1 str. 1, et 5; pom VI; kom 30B",
        "Moscow, Russian Federation (125373, Moscow, Pokhodniy Proezd, Domovladenie 3, str. 2)",
        "Murmansk, Russian Federation (Murmansk region, city of Murmansk, ter. Murmansk-17, 1)",
        "Nakhodka, Russian Federation",
        "Netherlands Antilles; Belize; United States of America",
        "Nizhniy Novgorod, Russian Federation",
        "Nizhny Novgorod region, Russian Federation",
        "Nizhny Novgorod, Russia",
        "Office B-405, B Wing, Mahavir Icon, Plot 89/90, Sector 15, CBD Belapur, Navi Mumbai, Maharashtra, India, 400614",
        "Oman",
        "Omsk, Russian Federation",
        "Pedestrian passage, home ownership 3. building 2, Moscow 125373, Russian Federation",
        "People’s Republic of China",
        "Perm, Perm region, Russian Federation",
        "Perm, Russian Federation",
        "Perova Polya proezd 9, building 1, Moscow, Russian Federation",
        "Petrovsk, Russian Federation",
        "Plast, Russia",
        "Pokhodnyy Proyezd 3-2, 125373, Moscow, Russian Federation",
        "Prague, Czech Republic",
        "Professora Popova st., 38, Saint Petersburg 197376, Russia",
        "Republic of Belarus",
        "Republic of Mauritius",
        "Reutov, Russian Federation",
        "Room 2-502, Building 140, 130, Minjiang Lu, Shinan Qu, Qingdao, Shandong, People’s Republic of China",
        "Rostov, Moscow, Russian Federation",
        "Russia",
        "Russian Federation",
        "Russian Federation (105094, Moscow, ul. Golianovskaia, 7A str. 1)",
        "Russian Federation (105275, Moscow, sh. Entuziastov, 29)",
        "Russian Federation (105318, Moscow, ul. Ibragimova, 31)",
        "Russian Federation (107023, Moscow, ul. Elektrozavodskaia, 24 str. 1.)",
        "Russian Federation (107241, Moscow, sh. Shchelkovskoe, 23A, pomeshch. V kom. 33.)",
        "Russian Federation (115068, Moscow, Velozavodskaya st., 5)",
        "Russian Federation (119121, Moscow, per. Truzhenikov 1-i, 16 str.17)",
        "Russian Federation (125183, Moscow, Bolshaya Akademicheskaya st., 73 k. 3, premises. 5n.)",
        "Russian Federation (141983, Moscow region, Dubna, Zhukovsky Str. 2, building 1)",
        "Russian Federation (183032, Murmansk Oblast, Murmansk, Zavodskaya Ulitsa, dom 7)",
        "Russian Federation (193091, Saint Petersburg, nab. Oktiabrskaia, 6,litera v, pomeshch. 8-n)",
        "Russian Federation (194021, Saint Petersburg, ul. Karbysheva, 15)",
        "Russian Federation (197022, Saint Petersburg, ul. Akademika Pavlova, 14)",
        "Russian Federation (197046, Saint Petersburg, ul. Malaia Posadskaia, 30)",
        "Russian Federation (197110, Saint Petersburg, ul. Pionerskaia, 44)",
        "Russian Federation (248600, Kaluga, Voskresenskiy per, 28)",
        "Russian Federation (305502, Kursk region, pos. Marshal Zhukov, military unit 32406)",
        "Russian Federation (416506, Astrakhan Region, Akhtubinsky District, Akhtubinsk, st. Sukhoi P.O., Building 12a)",
        "Russian Federation (425350, Republic of Mari El, city of Kozmodemyansk, str. Gagarina, 10)",
        "Russian Federation (450022, Republic of Bashkortostan, Ufa city, Mendeleev Street, 134)",
        "Russian Federation (606002, Nizhny Novgorod region, Dzerzhinsk city, Sverdlov avenue, 4)",
        "Russian Federation (606083, Nizhny Novgorod region, Volodarsky district, Mulino)",
        "Russian Federation (Krasnoyarsk kr., Zheleznogorsk, st. Lenina, d. ZD. 52)",
        "Russian Federation (Rostov region, Rostov-on-Don, st. Krasnoarmeiskaya, 9)",
        "Russian Federation 125284, Moscow, Leningradskiy Prospekt, d. 31a, str. 1",
        "Russian Federation, 105082, Moscow, Rubtsovskaya nab., 3 p. 1",
        "Russian Federation, 105120, Moscow, Syromyatnichesky pr-d, 6 k.1",
        "Russian Federation, 119019, Moscow, Vozdvizhenka Str. 18/9",
        "Russian Federation, 123298, Moscow, ulitsa Narodnogo Opolcheniya, d. 40, k. 2",
        "Russian Federation, 129226, Moscow, Mira Avenue, 125",
        "Russian Federation, 191014, St. Petersburg, Paradnaya st., 8",
        "Russian Federation, 393192, Tambov region, Kotovsk, Truda Ave., 23",
        "Russian Federation, 420032, Republic of Tatarstan, city of Kazan, Pervogo Maya Street, 14",
        "Russian Federation, 420036, Republic of Tatarstan, Kazan, Dementyeva st., 1",
        "Russian Federation, 424003, Republic of Mari El, Yoshkar-Ola, st. Suvorova, 15",
        "Russian Federation, 432030, Ulyanovsk region, Ulyanovsk city, Narimanov Ave., 75",
        "Russian Federation, 602205, Vladimir region, Murom, 30 Let Pobedy St., 1a.",
        "Russian Federation, 620024, Ekaterinburg, ulitsa Simkaya, d. 2",
        "Russian Federation, 620091, Sverdlovsk region, Yekaterinburg, Kosmonavtov Ave., building 18",
        "Russian Federation, 624981, Sverdlovsk region, Serov city, Aglomeratchikov str. 10",
        "Russian Federation, Krasnodar region, Krasnodar, st. Them. Fadeeva (Pashkovsky Residential Massif) Ter, 429/1 POD A, office 17",
        "Russian Federation, Moscow",
        "Russian Federation, Nizhny Novgorod region, Nizhny Novgorod, st. Nartova, d. 6 k. 6, room 25 cab. 96",
        "Russian Federation, Omsk region, Omsk, Karl Marx Avenue, 41",
        "Russian Federation, St. Petersburg",
        "Russian Federation, Sverdlovsk region, Yekaterinburg",
        "Russian Federation, Tula region, Tula",
        "Russian Federation, Vologda Oblast",
        "Russian Federation; Belize",
        "Russia’s occupied part of the Donbass region",
        "Ryazan, Russian Federation",
        "Saint Petersburg, Russian Federation",
        "Samara, Russian Federation",
        "Sarapul, Russian Federation",
        "Sevastopol, Crimea",
        "Sharjah, United Arab Emirates",
        "Singapore",
        "St Petersburg and Moscow, Russian Federation",
        "St Petersburg, Russian Federation",
        "St. Petersburg",
        "St. Petersburg, Russian Federation",
        "St. Petersburg, Russian Federation (PO Box 128, Saint Petersburg 197022)",
        "Tatarstan Republic, Russian Federation",
        "Tehran, Iran",
        "Trekhprudny 5, Moscow, Russian Federation",
        "Tula, Russia",
        "Tula, Russian Federation",
        "Türkiye",
        "Ukraine",
        "Ukraine (283001, Donetsk, Komsomolsky prospect, 6a)",
        "Ukraine (283048, Donetsk, st. Shchorsa, 60)",
        "United Arab Emirates",
        "United Kingdom",
        "United States of America",
        "Vietnam",
        "Vladivostok, Russian Federation",
        "Volgograd region, Volgograd, Russian Federation",
        "Votkinsk, Republic of Udmurt, Russia",
        "Wanchai, Hong Kong",
        "Xinxiang, Henan Province, China",
        "Yangzhou, People’s Republic of China",
        "Yekaterinburg, Russian Federation",
        "Zelenodolsk, Republic of Tatarstan, Russian Federation (422540, Republic of Tatarstan, r-n Zelenodolskii, Zelenodolsk, ul. Lenina, 41A)",
        "str. Butlerova, 13, letter A, room. 20Н, 195256, St. Petersburg, Russian Federation",
        "str. Fadeeva, 7 building 1, office 2, 125047, Moscow, Russian Federation",
        "str. M. Gorky 6, 300002, city of Tula, Tula region, Russian Federation",
    }
)

# Country-only business places are country associations, not street addresses.
# The remaining printed values stay in address pending individual review.
BUSINESS_LABELS = frozenset(
    {"Principal place of business", "Principal places of business"}
)
BUSINESS_TERRITORIES = REGISTRATION_TERRITORIES | {
    "Czech Republic",
    "Iran",
    "Liberia",
    "Republic of Azerbaijan",
}
BUSINESS_REVIEWED_VALUES = frozenset(
    {
        "13 km of Shahid Babaei Highway, after Imam Hossein University, next to Telo Road, Tehran, Iran",
        "Abu Dhabi, United Arab Emirates",
        "Arsenyev, Russian Federation",
        "Belarus",
        "Crimea",
        "Crimea, Ukraine",
        "Czech Republic",
        "Democratic People’s Republic of Korea",
        "Henan Province, People’s Republic of China",
        "Hong Kong",
        "Hong Kong Special Administrative Region of the People’s Republic of China",
        "India",
        "Iran",
        "Israel",
        "Kaspiysk, Republic of Dagestan, Russian Federation",
        "Kazakhstan",
        "Khimki, Russian Federation",
        "Krasnodar region, Krasnodar, Russian Federation",
        "Kuibyshev, Russian Federation",
        "Kumertau, Russian Federation",
        "Kyrgyzstan",
        "Liberia",
        "Moscow",
        "Moscow, Russia",
        "Moscow, Russian Federation",
        "Nakhodka, Russian Federation",
        "Non-government controlled areas of Ukraine, namely the oblasts of Donetsk and Luhansk (the so-called “Donetsk People’s Republic” and “Luhansk People’s Republic”), Kherson and Zaporizhzhia",
        "Oman",
        "People’s Republic of China",
        "People’s Republic of China/Hong Kong",
        "People’s Republic of China; Russian Federation",
        "Perm, Russian Federation",
        "Petrovsk, Russian Federation",
        "Republic of Azerbaijan",
        "Republic of Belarus",
        "Russia",
        "Russia, Georgia, Ukraine",
        "Russia, Moscow",
        "Russia, Ukraine",
        "Russian Federatio",
        "Russian Federation",
        "Russian Federation Associated entities:",
        "Russian Federation and Crimea",
        "Russian Federation, Krasnodar",
        "Russian Federation, Mytishchi",
        "Russian Federation, Netherlands",
        "Russian Federation, Nizhny Novgorod region, Nizhny Novgorod",
        "Russian Federation, Saratov",
        "Russian Federation, Tarusa",
        "Russian Federation, Temporarily occupied territory of Ukraine (Simferopol)",
        "Russian Federation, Tula",
        "Russian Federation, Yekaterinburg",
        "Russian Federation; Belize",
        "Russian-occupied regions of Zaporizhzhia and Kherson",
        "Russia’s occupied part of the Donbass region",
        "Singapore",
        "St Petersburg, Russian Federation",
        "Tula, Russian Federation",
        "Türkiye",
        "Ukraine",
        "Ukraine, Russian Federation",
        "United Arab Emirates",
        "United Arab Emirates, Russian Federation, European Union",
        "Volgograd, Russian Federation",
        "so-called ‘Donetsk People’s Republic’",
        "so-called ‘Luhansk People’s Republic’",
    }
)

# Colon-less document-attribute lines, dropped like their labelled kin.
COLONLESS_DROPS = ("Issued by ",)
# A label printed inside another label's value, as (outer, inner). The inner
# label names the more specific identifier and governs the column; it is
# never part of the value. Entries 997–999, 1001 and 1002 of M125 print
# "Registration number: BIN/OGRN: …".
NESTED_LABELS = frozenset({("Registration number", "BIN/OGRN")})
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document (roles spanning paragraphs, address lists, notes
# prose, an entity-form qualifier). Bare lines after any other label are
# new structure.
CONTINUABLE_COLUMNS = frozenset(
    {"position", "address", "notes", "phone", "website", "legalForm"}
)

# Entity entries whose printed identifiers land in Company-only columns
# (kppCode from the KPP/PPC/registration-reason-code labels, imoNumber,
# a stock ticker) are emitted as Company; the printed structure forces
# the schema.
COMPANY_PROPS = ("kppCode", "imoNumber", "ticker")

# Entries whose parenthesized rendering is printed with its closing
# parenthesis lost (a recurring amending-act misprint, 881 precedent): the
# group still open at the cell end is closed under review.
UNCLOSED_GROUP_PINS = frozenset(
    {
        ("PERSONS", "394"),
        ("PERSONS", "1475"),
        ("PERSONS", "1725"),
        ("ENTITIES", "18"),
        ("ENTITIES", "135"),
        ("ENTITIES", "202"),
        ("ENTITIES", "300"),
    }
)
# Two entries print a language-labelled rendering wrapped mid-phrase onto
# the next line; the fragment is joined to its rendering.
NATIVE_WRAP_PINS = frozenset({("ENTITIES", "560"), ("ENTITIES", "597")})
# One entry ends its name cell with an a.k.a. label whose value was never
# printed (misprint); the dangling label is dropped under review.
DANGLING_AKA_PINS = frozenset({("ENTITIES", "691")})
# Reviewed hand-mappings for name-cell lines the rendering rules cannot
# place, keyed by (part, entry) and the exact line.
NAME_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # A rendering group nesting an a.k.a. annotation with an internal ";"
    # (the ";" separates the two aliases inside the nested annotation).
    ("PERSONS", "140"): {
        "(Сергей Юрьевич КУЗОВЛЕВ": (("alias", "Сергей Юрьевич КУЗОВЛЕВ"),),
        "(a.k.a. Сергей; ИГНАТОВ, ТAMБOB))": (
            ("alias", "Сергей"),
            ("alias", "ИГНАТОВ, ТAMБOB"),
        ),
    },
    # The rendering's parenthesis closes mid-line and the listing extends
    # to the enterprise's branches (Belarus B25 precedent: inclusion lines
    # are notes; the branch names follow one per line).
    ("ENTITIES", "41"): {
        (
            "(‘Государственное унитарное предприятие Республики Крым "
            "‘Крымские морские порты’’), including branches:"
        ): (
            (
                "alias",
                "‘Государственное унитарное предприятие Республики Крым "
                "‘Крымские морские порты’’",
            ),
            ("notes", "including branches:"),
        ),
        "Feodosia Commercial Port,": (("notes", "Feodosia Commercial Port"),),
        "Kerch Ferry,": (("notes", "Kerch Ferry"),),
        "Kerch Commercial Port": (("notes", "Kerch Commercial Port"),),
    },
}

# Reviewed hand-mappings for identifying-information lines the rules cannot
# place, keyed by (part, entry) and the exact line. An empty mapping drops
# the line deliberately. Populated by the fail-closed iteration.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # A second, possibly-alternative birth place printed on a bare line.
    ("PERSONS", "132"): {
        (
            "or possibly Hirnytskyi village, Perevalsk district, Luhansk "
            "oblast, USSR (now Ukraine)"
        ): (
            (
                "birthPlace",
                "or possibly Hirnytskyi village, Perevalsk district, "
                "Luhansk oblast, USSR (now Ukraine)",
            ),
        ),
    },
    # The birth place's native rendering on a bare line.
    ("PERSONS", "142"): {
        "Невинномысск, Ставропольский край, Российская Федерация": (
            (
                "birthPlace",
                "Невинномысск, Ставропольский край, Российская Федерация",
            ),
        ),
    },
    # The DOB's year qualifier printed on its own line.
    ("PERSONS", "1017"): {
        "(year unknown)": (("birthDate", "(year unknown)"),),
    },
    # An unlabelled identifier line mixing a national ID with an
    # alternative passport (contiguous spans extracted under review).
    ("PERSONS", "1409"): {
        "654034325 (Russia); alt. Passport XXIIAH534753": (
            ("idNumber", "654034325 (Russia)"),
            ("passportNumber", "XXIIAH534753"),
        ),
    },
    # An award inside the other-identifying-information value; the award
    # label has no column and the prose reads bare.
    ("PERSONS", "1509"): {
        (
            "Other identifying information: Awards: Certificate of Merit "
            "of the President of the Russian Federation"
        ): (
            (
                "notes",
                "Certificate of Merit of the President of the Russian Federation",
            ),
        ),
    },
    # The passport label is glued to the INN value with no line break.
    ("PERSONS", "2099"): {
        "INN: 732508747179 Passport number: 7304202860": (
            ("innCode", "732508747179"),
            ("passportNumber", "7304202860"),
        ),
    },
    # The nationality label is printed with neither colon nor value
    # (misprint); a value is never invented, so the label is dropped.
    ("PERSONS", "2172"): {
        "Nationality": (),
    },
    # The birth place printed on a bare line without its label.
    ("PERSONS", "1766"): {
        "Novomoskovsk, Tula Oblast, USSR (now Russian Federation)": (
            (
                "birthPlace",
                "Novomoskovsk, Tula Oblast, USSR (now Russian Federation)",
            ),
        ),
    },
    # The info cell opens with the transliterated corporate name and an
    # a.k.a. line (name renderings printed in the wrong column).
    ("ENTITIES", "235"): {
        (
            "Obshchestvo S Ogranichennoi Otvetstvennostiu "
            "Nauchno-Tekhnicheskii Tsentr ‘Poisk-IT’"
        ): (
            (
                "alias",
                "Obshchestvo S Ogranichennoi Otvetstvennostiu "
                "Nauchno-Tekhnicheskii Tsentr ‘Poisk-IT’",
            ),
        ),
    },
    # A misprinted, truncated tax label ("TIN/:").
    ("ENTITIES", "346"): {
        "TIN/: 7718016666": (("taxNumber", "7718016666"),),
    },
    # A second registration event continuing the ";"-terminated value.
    ("ENTITIES", "477"): {
        "10.7.2023 (registered as a small enterprise)": (
            ("incorporationDate", "10.7.2023 (registered as a small enterprise)"),
        ),
    },
    # A dash-separated tax identifier line.
    ("ENTITIES", "697"): {
        "Russian Tax Identification Number - 0268061215": (
            ("taxNumber", "0268061215"),
        ),
    },
    # Unlabelled proceedings-status prose about the entity itself.
    ("ENTITIES", "38"): {
        "Ongoing bankruptcy proceedings.": (
            ("notes", "Ongoing bankruptcy proceedings."),
        ),
    },
    ("ENTITIES", "40"): {
        "In process of liquidation": (("notes", "In process of liquidation"),),
    },
}
# Entries whose date cell prints a stray trailing period or semicolon (list
# punctuation, not date wording), or nothing at all.
DATE_PERIOD_PINS = frozenset(
    {("PERSONS", "706"), ("PERSONS", "710"), ("PERSONS", "1819"), ("PERSONS", "1917")}
)
DATE_EMPTY_PINS = frozenset({("ENTITIES", "103")})
# One reasons paragraph is printed as its own single-cell row.
SINGLE_CELL_REASON_PINS = frozenset({("PERSONS", "885")})
# One entry number is printed without the trailing full stop that every
# other entry carries.
DOTLESS_NUMBER_PINS = frozenset({("PERSONS", "2097")})
# Two rows print a continuation with one column missing entirely: entry
# 18's drops the date column (four cells = number/name/info/reasons),
# entry 434's drops the number column (four cells = name/info/reasons/date).
FOUR_CELL_CONTINUATION_PINS = {
    ("ENTITIES", "18"): "date-missing",
    ("ENTITIES", "434"): "number-missing",
}


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def enclosing_parens(text: str) -> bool:
    """True when the first "(" pairs with the final ")" around the whole."""
    if not (text.startswith("(") and text.endswith(")")):
        return False
    depth = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index == len(text) - 1
    return False


def add_rendering(ctx: str, row: Row, text: str) -> None:
    """File one printed rendering: previousName by label, else alias.
    Trailing list punctuation, enclosing parentheses, a.k.a. labels and
    language annotations are printed structure, never part of the value;
    a lone unbalanced trailing parenthesis is a print artifact and shed."""
    text = text.strip().rstrip(",;").strip()
    if enclosing_parens(text):
        text = text[1:-1].strip()
    fka = FKA_RE.match(text)
    if fka is not None:
        row.add("previousName", [fka.group(1)])
        return
    for value in split_values(text):
        for piece in AKA_SPLIT_RE.split(value):
            piece = piece.strip().rstrip(",;").strip()
            while True:
                stripped = AKA_STRIP_RE.sub("", piece)
                if stripped == piece:
                    break
                piece = stripped.strip()
            lang = LANG_PREFIX_RE.match(piece)
            if lang is not None:
                piece = lang.group(1)
            if enclosing_parens(piece):
                piece = piece[1:-1].strip()
            if piece.endswith(")") and "(" not in piece:
                piece = piece[:-1].strip()
            if piece:
                row.add("alias", [piece])


def parse_name_extras(
    ctx: str, part: str, record_id: str, row: Row, lines: list[str]
) -> None:
    """Renderings printed under the name line: every line is an alias
    (or previousName by printed label); parenthesized groups may wrap."""
    overrides = NAME_OVERRIDES.get((part, record_id), {})
    group: list[str] | None = None
    pending_aka = False
    pending_lang = False
    for line in lines:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            group, pending_aka, pending_lang = None, False, False
            continue
        if group is not None:
            close = GROUP_CLOSE_RE.match(line)
            if close is not None:
                group.append(close.group(1))
                add_rendering(ctx, row, " ".join(group))
                group = None
            else:
                group.append(line)
            continue
        if pending_aka or pending_lang:
            pending_aka = pending_lang = False
            add_rendering(ctx, row, line)
            continue
        if BARE_AKA_RE.match(line):
            pending_aka = True
            continue
        aka = AKA_RE.match(line)
        if aka is not None:
            add_rendering(ctx, row, aka.group(1))
            continue
        lang_line = LANG_LINE_RE.match(line)
        if lang_line is not None:
            value = lang_line.group(1)
            if value == "":
                pending_lang = True
            elif (part, record_id) in NATIVE_WRAP_PINS:
                group = [value]
                continue
            else:
                add_rendering(ctx, row, value)
            continue
        paren = PAREN_LINE_RE.match(line)
        if paren is not None:
            add_rendering(ctx, row, paren.group(1))
            continue
        if line.startswith("("):
            group = [line[1:]]
            continue
        if line[0].islower():
            if (part, record_id) in NATIVE_WRAP_PINS:
                raise ParseError(f"{ctx}: wrap pin without open rendering")
            raise ParseError(f"{ctx}: suspected wrapped name line {line[:60]!r}")
        add_rendering(ctx, row, line)
    if group is not None:
        if (part, record_id) in NATIVE_WRAP_PINS:
            add_rendering(ctx, row, " ".join(group))
        elif (part, record_id) in UNCLOSED_GROUP_PINS:
            add_rendering(ctx, row, " ".join(group))
        else:
            raise ParseError(f"{ctx}: unterminated parenthesized rendering")
    if pending_aka:
        if (part, record_id) not in DANGLING_AKA_PINS:
            raise ParseError(f"{ctx}: dangling bare a.k.a. label")
    if pending_lang:
        raise ParseError(f"{ctx}: dangling language annotation")


def parse_name(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    # The first line is the name, kept whole: inline parentheticals and
    # a.k.a. tails are part of the printed combined form (the crawler's
    # name review categorises them).
    row.add("name", [lines[0]])
    parse_name_extras(ctx, part, record_id, row, lines[1:])


URL_RE = re.compile(r"^(?:https?:|www\.|t\.me/)\S+$")
PHONE_RE = re.compile(r"^\(?\+[\d\s()./,+-]+$")
EMAIL_RE = re.compile(r"^\S+@\S+$")


def classify_bare(line: str) -> tuple[str, str] | None:
    """Shape-classify a bare info line the label rules cannot place."""
    for long_label, column in LONG_LABELS.items():
        if line.startswith(long_label):
            return (column, line[len(long_label) :])
    if URL_RE.match(line):
        return ("website", line)
    if PHONE_RE.match(line):
        return ("phone", line)
    if EMAIL_RE.match(line.rstrip(",;")):
        return ("email", line.rstrip(",;"))
    for prefix, column in COLONLESS_PREFIXES:
        if line.startswith(prefix):
            return (column, line[len(prefix) :])
    return None


def add_registration(ctx: str, row: Row, value: str) -> str:
    """Place a reviewed registration value in its source-supported property."""
    if value not in REGISTRATION_REVIEWED_VALUES:
        raise ParseError(f"{ctx}: unreviewed place of registration {value[:70]!r}")
    territories = split_values(value)
    if all(territory in REGISTRATION_TERRITORIES for territory in territories):
        row.add("jurisdiction", territories)
        return "jurisdiction"
    # Semicolons can be components of one street address (entry 241).
    row.add("address", [value])
    return "address"


def add_business_place(ctx: str, row: Row, value: str) -> str:
    """Place a reviewed business location in its source-supported property."""
    if value not in BUSINESS_REVIEWED_VALUES:
        raise ParseError(
            f"{ctx}: unreviewed principal place of business {value[:70]!r}"
        )
    places = split_values(value)
    if all(place in BUSINESS_TERRITORIES for place in places):
        row.add("country", places)
        return "country"
    row.add("address", places)
    return "address"


def parse_info(
    ctx: str, part: str, record_id: str, td: Element, row: Row
) -> tuple[set[str], bool]:
    """Parse the identifying-information cell; returns (labels seen,
    whether any line remained unplaced — always an error upstream)."""
    lines = cell_lines(td, ctx)
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    seen: set[str] = set()
    block: str | None = None
    dropped = False
    seen_label = False
    opened_empty = False
    wrapped: str | None = None
    for line in lines:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            block, dropped, opened_empty, wrapped = None, False, False, None
            continue
        if (
            part,
            record_id,
            line,
        ) == ("ENTITIES", "541", "Known Garantex blockchain wallet addresses:"):
            # This unsupported wallet heading exceeds LABELLED_RE's label cap.
            block, dropped, seen_label = None, True, True
            opened_empty, wrapped = False, None
            continue
        if PAREN_LINE_RE.match(line) and block in {"address", "jurisdiction"}:
            # A parenthesized address detail continues the address block.
            row.add("address", [line])
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1).strip() if labelled is not None else None
        if label in ("https", "http"):
            label, labelled = None, None
        if block == "registration" and opened_empty and labelled is not None:
            raise ParseError(f"{ctx}: empty place of registration")
        if label in DROP_LABELS:
            block, dropped, seen_label = None, True, True
            opened_empty, wrapped = False, None
            seen.add(label or "")
            continue
        if label in NOTES_LABELS:
            assert labelled is not None
            seen.add(label or "")
            value = labelled.group(2)
            block, dropped, seen_label = "notes", False, True
            opened_empty, wrapped = False, None
            if value == "":
                continue
            routed = classify_bare(value) if LABELLED_RE.match(value) is None else None
            inner = LABELLED_RE.match(value)
            inner_label = inner.group(1).strip() if inner is not None else None
            if (
                inner is not None
                and inner_label is not None
                and inner_label in INFO_LABELS
            ):
                row.add(INFO_LABELS[inner_label], split_values(inner.group(2)))
                block = INFO_LABELS[inner_label]
            elif routed is not None:
                row.add(routed[0], [routed[1]])
            elif inner_label is not None and inner_label in DROP_LABELS:
                block, dropped = None, True
            elif inner_label is not None:
                raise ParseError(
                    f"{ctx}: unreviewed label inside notes value {line[:70]!r}"
                )
            else:
                row.add("notes", [value])
            continue
        if label in REGISTRATION_LABELS:
            assert labelled is not None
            seen.add(label)
            value = labelled.group(2)
            block = add_registration(ctx, row, value) if value else "registration"
            dropped, seen_label, opened_empty, wrapped = False, True, not value, None
            continue
        if label in BUSINESS_LABELS:
            assert labelled is not None
            seen.add(label)
            block = add_business_place(ctx, row, labelled.group(2))
            dropped, seen_label, opened_empty, wrapped = False, True, False, None
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            seen.add(label)
            column = INFO_LABELS[label]
            value = labelled.group(2)
            nested = LABELLED_RE.match(value)
            nested_label = nested.group(1).strip() if nested is not None else None
            if nested is not None and (label, nested_label) in NESTED_LABELS:
                column = INFO_LABELS[nested.group(1).strip()]
                value = nested.group(2)
            if value != "":
                row.add(column, split_values(value))
            block, dropped, seen_label = column, False, True
            # An empty-valued label holds its value on the following bare
            # line(s); a value ending in "," wraps mid-phrase onto the next
            # line (both 2020R1998 precedents).
            opened_empty = value == "" or value.endswith(";")
            wrapped = column if value.endswith(",") else None
            continue
        if block == "registration" and opened_empty:
            block = add_registration(ctx, row, line)
            opened_empty = False
            continue
        if line.startswith("Place of Registration "):
            seen.add("Place of Registration")
            block = add_registration(ctx, row, line[len("Place of Registration ") :])
            dropped, seen_label, opened_empty, wrapped = False, True, False, None
            continue
        aka = AKA_RE.match(line)
        if aka is not None:
            # Name renderings occasionally print inside the info cell; the
            # printed a.k.a. label marks them.
            add_rendering(ctx, row, aka.group(1))
            block, dropped, opened_empty, wrapped = None, False, False, None
            continue
        shaped = classify_bare(line)
        if shaped is not None:
            row.add(shaped[0], [shaped[1]])
            block, dropped, opened_empty, wrapped = shaped[0], False, False, None
            continue
        if any(line.startswith(prefix) for prefix in COLONLESS_DROPS):
            block, dropped, opened_empty, wrapped = None, True, False, None
            continue
        if dropped:
            continue
        if wrapped is not None:
            row.props[wrapped][-1] = f"{row.props[wrapped][-1]} {line}"
            wrapped = None
            continue
        if not seen_label:
            # Unlabeled leading lines: role prose for persons, the address
            # for entities (both shapes shown by the document).
            row.add("position" if part == "PERSONS" else "address", [line])
            continue
        if block is not None and (opened_empty or block in CONTINUABLE_COLUMNS):
            row.add(block, split_values(line))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:70]!r}")
    return seen, False


def entry_schema(part: str, default: str, row: Row) -> str:
    if part == "ENTITIES" and any(prop in row.props for prop in COMPANY_PROPS):
        return "Company"
    return default


def parse_date_cell(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        if (part, record_id) in DATE_EMPTY_PINS:
            return
        raise ParseError(f"{ctx}: empty date cell")
    if len(lines) != 1:
        raise ParseError(f"{ctx}: {len(lines)} lines in date cell")
    value = lines[0]
    if (part, record_id) in DATE_PERIOD_PINS:
        value = value.rstrip(".;")
    row.start_date = verbatim_date(value, ctx, DATE_FORMATS)


def continue_row(ctx: str, part: str, row: Row, cells: list[Element]) -> None:
    """Merge a continuation row (empty number cell) into the previous
    entry: further renderings, identifying information, reasons prose."""
    ctx = f"{ctx} entry {row.record_id} (cont.)"
    name_lines = cell_lines(cells[0], ctx)
    if name_lines:
        parse_name_extras(ctx, part, row.record_id, row, name_lines)
    info_lines = cell_lines(cells[1], ctx)
    if info_lines:
        parse_info(ctx, part, row.record_id, cells[1], row)
    reason_lines = cell_lines(cells[2], ctx)
    if reason_lines:
        row.reason = (
            " ".join([row.reason, *reason_lines])
            if row.reason
            else " ".join(reason_lines)
        )
    if len(cells) > 3:
        date_lines = cell_lines(cells[3], ctx)
        if date_lines:
            raise ParseError(f"{ctx}: unexpected date content in continuation row")


def sort_key(record_id: str) -> tuple[int, str]:
    match = NUMBER_RE.match(f"{record_id}.")
    assert match is not None
    return (int(match.group(1)), match.group(2))


def parse_part(roman: str, part: str, default_schema: str, table: Element) -> list[Row]:
    ctx = f"{roman}.{part}"
    rows: list[Row] = []
    trs = xpath_elements(table, ".//tr")
    if not trs:
        raise ParseError(f"{ctx}: table has no rows")
    first = tuple(
        clean(element_text(td), ctx) for td in xpath_elements(trs[0], "./td|./th")
    )
    if first != HEADER:
        raise ParseError(f"{ctx}: header {first} != expected {HEADER}")
    last: Row | None = None
    for tr in trs[1:]:
        cells = xpath_elements(tr, "./td|./th")
        if len(cells) == 1:
            text = " ".join(element_text(cells[0]).split())
            if (
                last is not None
                and (part, last.record_id) in SINGLE_CELL_REASON_PINS
                and not text.startswith("▼")
            ):
                last.reason = f"{last.reason} {clean(text, ctx)}"
                continue
            check_marker(text, ctx)
            continue
        if len(cells) == 4:
            shape = (
                None
                if last is None
                else FOUR_CELL_CONTINUATION_PINS.get((part, last.record_id))
            )
            if last is None or shape is None:
                raise ParseError(f"{ctx}: unpinned four-cell row")
            if shape == "date-missing":
                if cell_lines(cells[0], ctx):
                    raise ParseError(f"{ctx}: number content in four-cell row")
                continue_row(ctx, part, last, cells[1:])
            else:
                continue_row(ctx, part, last, cells)
            continue
        if len(cells) != len(HEADER):
            raise ParseError(f"{ctx}: row has {len(cells)} cells")
        lines = [cell_lines(c, ctx) for c in cells]
        if not any(lines):
            continue
        number_lines = lines[0]
        if not number_lines:
            if last is None:
                raise ParseError(f"{ctx}: continuation row before first entry")
            continue_row(ctx, part, last, cells[1:])
            continue
        if len(number_lines) != 1:
            raise ParseError(f"{ctx}: {len(number_lines)} lines in number cell")
        number = number_lines[0]
        if (part, number) in DOTLESS_NUMBER_PINS:
            number = f"{number}."
        match = NUMBER_RE.match(number)
        if match is None:
            raise ParseError(f"{ctx}: unrecognized entry number {number_lines[0]!r}")
        record_id = match.group(1) + match.group(2)
        entry_ctx = f"{ctx} entry {record_id}"
        row = Row(f"{roman}.{part}", default_schema, MEASURE, record_id=record_id)
        parse_name(entry_ctx, part, record_id, cells[1], row)
        parse_info(entry_ctx, part, record_id, cells[2], row)
        row.reason = " ".join(cell_lines(cells[3], entry_ctx))
        parse_date_cell(entry_ctx, part, record_id, cells[4], row)
        rows.append(row)
        last = row
    keys = [sort_key(r.record_id) for r in rows]
    if keys != sorted(keys) or len(set(keys)) != len(keys):
        raise ParseError(f"{ctx}: entry numbers not strictly increasing")
    # Schema is decided once continuations have landed: Company-only
    # identifier columns force the schema.
    for row in rows:
        row.schema = entry_schema(part, default_schema, row)
    return rows


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    part_index = -1
    part_tables = [0 for _ in PARTS]
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), roman)
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            heading = clean(element_text(child), roman)
            if heading != SUBTITLE:
                raise ParseError(f"{roman}: unexpected subtitle {heading!r}")
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            part_index += 1
            if part_index >= len(PARTS):
                raise ParseError(f"{roman}: more part headings than parts")
            heading = clean(element_text(child), roman)
            if heading != PARTS[part_index][0]:
                raise ParseError(f"{roman}: unexpected part heading {heading!r}")
            continue
        if child.tag == "div" and cls == "centered":
            if part_index < 0:
                raise ParseError(f"{roman}: table before first part heading")
            _, part, schema = PARTS[part_index]
            part_tables[part_index] += 1
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            rows.extend(parse_part(roman, part, schema, table))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if part_tables != [1 for _ in PARTS]:
        raise ParseError(f"{roman}: part table counts {part_tables}, expected one each")
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for roman, block in annex_blocks(doc, {"I"} | NON_TARGET):
        if roman in NON_TARGET:
            continue
        annex_rows = parse_annex_i(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 269/2014 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(
            PROGRAM_KEY, [MEASURE], [part[2] for part in PARTS] + ["Company"]
        )
        check_consolidated_celex(celex, FRAMEWORK_CELEX)
        content = load_source(celex, source)
        doc = html.fromstring(content)
        rows = parse_document(doc)
        records = [to_record(row, FRAMEWORK_CELEX, PROGRAM_KEY) for row in rows]
        validate_records(records)
        csv_path = write_csv(records, FRAMEWORK_CELEX)
        click.echo(json.dumps(summary(records, celex), indent=2))
        click.echo(f"wrote {csv_path}")
    except ParseError as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    main()
