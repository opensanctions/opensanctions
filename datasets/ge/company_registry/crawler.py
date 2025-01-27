import csv
from normality.cleaning import collapse_spaces
from followthemoney.types import registry

from zavod import Context, helpers as h
from zavod.shed.internal_data import fetch_internal_data, list_internal_data

# ORGS: Cooperative, Foreign Non-Commercial Legal Entity Branch, Public Legal Entity
LEGAL_FORMS = {
    "ეზღუდული პასუხისმგებლობის საზოგადოება",  # Limited Liability Company
    "კოოპერატივი",  # Cooperative
    "უცხოური არასამეწარმეო იურიდიული პირის ფილიალი",  # Foreign Non-Commercial Legal Entity Branch
    "უცხოური საწარმოს ფილიალი",  # Foreign Commercial Entity Branch
    "სააქციო საზოგადოება",  # Joint Stock Company
    "საჯარო სამართლის იურიდიული პირი",  # Public Legal Entity
    "სოლიდარული პასუხისმგებლობის საზოგადოება",  # Solidarity Limited Liability Company
    "კომანდიტური საზოგადოება",  # Limited Partnership
    "შეზღუდული პასუხისმგებლობის საზოგადოება",  # General Partnership
}
ORGS = {
    "კოოპერატივი",  # Cooperative
    "უცხოური არასამეწარმეო იურიდიული პირის ფილიალი",  # Foreign Non-Commercial Legal Entity Branch
    "საჯარო სამართლის იურიდიული პირი",  # Public Legal Entity
}
COUNTRIES = {
    "კრიტიკული",  # Critical
    "ღოლამრეზა შიროუჟან",
    "კორეისრესპუბლიკა",  # Korea
    "ლუქსემბურგი",  # Luxembourg
    "არაბთა გაერთიანებული საემიროები",  # United Arab Emirates
    "ცილიანგ",
    "ბოსტვანა",  # Botswana
    "რუსეთისფედერაცია",  # Russia
    "საუდის არაბეთი",  # Saudi Arabia
    "ჰონკონგი",  # Hong Kong
    "ბავშვთა დახმარების საერთაშორისო ასოციაცია",  # Save the Children
    "ლისამოლი",  # Lesotho
    "IIIსართ",  # III
    "სლოვაკეთი",  # Slovakia
    "ეგვიპტე ექსპორტი და ინპორტის მენეჯერი",  # Egypt Export and Import Manager
    "ინტერნეიშენალ ბიზნეს გრუპი''",  # International Business Group
    'ტოიოტა ცენტრი თბილისი"',  # Toyota Center Tbilisi
    "დიდი ბრიტანეთისა და ჩრდილოეთ ირლანდიის გაერთიანებული",  # United Kingdom
    "ცარ ასენის",  # Char Assen
    "ენ",  # En
    "ნლXX",  # NLXX
    "საქართველო",  # Georgia
    "ჰანთინგთონშტეინშ",  # Huntingtons
    "მესართ ოთახი H",  # Mesart House H
    "დიდი ბრიტანეთისა და ჩრდილოეთ ირლანდიის გაერთიანებული სამეფო",  # United Kingdom
    "არუბა",  # Aruba
    "პორტუგალია",  # Portugal
    "ჰანი ჰედაიან ს",  # Hani Hedaians
    "# რაგნალ ჰაუსი",  # Raghnal House
    "სალვადორი",  # Salvador
    "კატარი",  # Qatar
    "ფინეთი",  # Finland
    "დიდი ბრიტანეთის და ჩრდილოეთ ირლანდიის გაერთიანებული სამეფო",  # United Kingdom
    "სლოვენია",  # Slovenia
    "ლიეტუვა (ლიტვა)",  # Lithuania
    "სინტ ტაუნისლაან",
    "ჯიბუტი",  # Djibouti
    "გამგეობის თავმჯდომარის მოადგილე განათლების დარგში",  # Head of the Department of Education
    "ფატემენ აზარფარ",  # Fatemen Azarfar
    "ეგვიპტე გენერალური მენეჯერი",  # Egypt General Manager
    "რექლამ ვე დანიშმანლიქ ჰიზთემლერი",  # Advertisement and Marketing Manager
    "გრენადა",  # Grenada
    "ქუვეითი",  # Kuwait
    "სუდანი",  # Sudan
    "მაჰდი საედოლზაქერინი",  # Mahdi Saedolzakerin
    "დიდი ბრიტანეთისადა ჩრდილოეთ ირლანდიის გაერთიანებულისამეფო",  # United Kingdom
    "გოდერძი ქართველიშვილი",  # Godezri Kartvelishvili
    "მატრავი აჰმედ აბდუ იოსსეფ",  # Matravi Ahmed Abdul Joseph
    "მოჰამმედ ჯადდუა მ",  # Mohamed Jadua M
    "სიერალეონე",  # Sierra Leone
    "მავრიკი",  # Mauritius
    "ომანი",  # Oman
    "#ა",  # A
    #############################################
    "ბესიკი",  # Basic
    "მაროკო",  # Morocco
    "გენერალური მდივანი",  # General Manager
    "იტალია",  # Italy
    "დიდიბრიტანეთი",  # Great Britain
    "მონტენეგრო",  # Montenegro
    "სალომე დე პულპიკე დუ ალგუეტი",  # Salome de Pulpike du Algueti
    "საფ განყ",  # SAFF GANQ
    "პალესტინა",  # Palestine
    "სამაჰ საად სარკის ჰანა",  # Samah Saad Sarkis Hana
    "დიდი ბრიტანეთისადა ჩრდილოეთ ირლანდიის გაერთიანებული სამეფო",  # United Kingdom
    "ანნა სეიედჯავად",  # Anna Seiedjavadi
    "სიერა-ლეონე",  # Sierra Leone
    "ირანი(ისლამური რესპუბლიკა)",  # Iran (Islamic Republic of)
    "ახალი ზელანდია",  # New Zealand
    "კახა დვალი ()",  # Kaha Dvali
    "ბელორუსია",  # Belarus
    "კოლუმბია",  # Colombia
    "დიდი ბრიტანეთისადა ჩრდილოეთ ირლანდიისგაერთიანებული სამეფო",  # United Kingdom
    "რუსეთის ფედერაცია",  # Russia
    "ბელარუსი",  # Belarus
    "ინშუარენს რეინშუარენს ბროიუქერზ",  # Insurance Reinsurance Brokers
    "პერუ",  # Peru
    "პოლეტ",  # Polet
    "აბდულკარიმ ალი მ",  # Abdulkarim Ali M
    "სან-მარინო",  # San Marino
    "ჟეკი",  # Zheki
    'ჯავახავტოგზა"',  # Javakhtogza
    'გორგოტა"',  # Giorgota
    "ურუგვაი",  # Uruguay
    "ა შ შ  (USA)",  # USA
    "რეიმონ",  # Raymond
    "ავსტრალია",  # Australia
    "კვიპროსი",  # Cyprus
    "დრეიკ ჩემბერსი",  # Drake Chambers
    "აბდულჰაკემ ალი ერ",  # Abdulhakem Ali Er
    "ოსკიანი",  # Oscar
    "ტრიდენტ ჩემბერს",  # Trident Chambers
    "ალი ქელლეჯი",  # Ali Kelleji
    "საფოსტო ყუთი",  # Post Box
    "ურუმაშვილი და პარტნიორები",  # Urumashvili and Partners
    "ჟენევა პალასი",  # Geneva Palace
    "კლოდ ბეივე",  # Claude Beave
    "ალი ვაჰდანი",  # Ali Vahdani
    "ისრაელი",  # Israel
    "ტანზანიის გაერთიანებული რესპუბ",  # United Republic of Tanzania
    "კუბა",  # Cuba
    "ჩეხეთის რესპუბლიკა",  # Czech Republic
    "ინდოეთი",  # India
    "აზერბაიჯანი",  # Azerbaijan
    "როია რასულვანდ სადეყი",  # Roia Rasulvand Sadeqi
    "იაპონია",  # Japan
    "ბარკლის ნავსადგური",  # Barclay's Bank
    "ჯერსი",  # Jersey
    #############################################
    "ნიდერლანდები",  # Netherlands
    "სმიგან თრეიდინგ ლიმიტედ''",  # Smigun Trading Limited
    "მონღოლეთი",  # Mongolia
    "ბიზნესმენთა და მომხმარებელთა ინტერესების დაცვის კავშირი",  # Businessmen and Users Interest Protection Association
    "ანგოლა",  # Angola
    "შვედეთი",  # Sweden
    "მოჰამმად აჰმადვანდ",  # Mohamed Ahmadvand
    "თავისუფალი ზონა",  # Free Zone
    "სამოა",  # Samoa
    "თარიქ ეიდ",  # Tarik Eid
    "ჰონდურასი",  # Honduras
    "რეზვან ფერდოს",  # Rezvan Ferdos
    "კორეის სახალხოდემოკრატიული რე",  # Democratic People's Republic of Korea
    "ყოლამრეზა აბედითამეჰ",  # Kolamreza Abeditameh
    "პატრიკ გორვიცი",  # Patrick Gorvichi
    "ბინ",  # Bin
    "სიმიდოუ ჰაუსი",  # Simidou House
    "სთეფენ ქორთ",  # Stefen Kort
    "მექსიკა",  # Mexico
    "ა შ შ",  # USA
    'შპს "შენონ ეირ ინტერნეიშნლ"',  # Shenon Air International
    "ეკვადორი",  # Ecuador
    "ფეთიჰის უბ სონმეზისქ",  # Petihi's UB Sonmezisk
    "ბრაზილია",  # Brazil
    "პანამა",  # Panama
    "უკრაინა",  # Ukraine
    "ა",  # A
    "ამირ ჰოსსეინ გოლჩინ ნია",  # Amir Hossein Golchin Nia
    "გასასვლელი",  # Departure
    "ნიგერია",  # Nigeria
    "არაბთაგაერთიანებული საამიროებ",  # United Arab Emirates
    "სანაი",  # Senai
    "ტრინიდადი და ტობაგო",  # Trinidad and Tobago
    "ყატარი",  # Qatar
    'ქართული შაქარი"',  # Georgian Shakari
    "დომინიკა",  # Dominica
    "მიად დარაეი",  # Miad Daraei
    "ინდონეზია",  # Indonesia
    "ანტიგუა და ბარბუდა",  # Antigua and Barbuda
    "ცორტ როუ",  # Court Row
    "სენტ-ლუსია",  # Saint Lucia
    "ს ა",  # SA
    "ზაჰრა ხოდაპარასთ ხიაბანი",  # Zahra Khodaparast Khiabani
    "ნიუე",  # Niue
    "ადელ აბდულმოჰსენ ჰ",  # Adel Abdulmohsen H
    "საფეიქროწარმოება",
    "Saint George''",
    "ბაჰამის კუნძულები",  # Bahamas
    "კაია რიშარდ ჯ  ბიუჯონ ზნ",  # Kaia Richard J. Buchanan Zn
    "ალჟირი",  # Algeria
    "ბერტრან",  # Bertran
    "ირანი",  # Iran
    "ალბანეთი",  # Albania
    "სანმარინო",  # San Marino
    "ლაოსი",  # Laos
    'ალპინ დეველოპმენტ გრუპ"',  # Alpine Development Group
    "მათინ სადეღი",  # Matin Sadeghi
    "კანცლერი",  # Canceller
    "სერბეთი",  # Serbia
    "იონგჩი",  # Yongchi
    "სურინამი",  # Suriname
    "მაჰმუდ ჰუსეინი აბდელკადერირაქი",  # Mahmud Hussein Abdelkaderiraki
    "კამბოჯა",  # Cambodia
    "გ",  # G
    "რუსეთის ფედერაცია ფინანსურ საკითხებში",  # Russia Federation Financial Issues
    "სააქციო საზოგადოება ხრამჰესი",  # Public Service Khramhesi
    "ავღანეთი",  # Afghanistan
    "ფეიმან ქაზემი",  # Fayman Kazemi
    "სენეგალი",  # Senegal
    "მარშალისკუნძულები",  # Marshall Islands
    "ანდრიაპავლიდის",  # Andriapavlidis
    'მესხეთავტოგზა"',  # Meskhetogza
    "ატოლი",  # Atoli
    "ავსტრია",  # Austria
    "V სართ",  # V III
    "შვეიცარია",  # Switzerland
    "დრეიკის პალატა",  # Drake Palace
    "ეივაზ მინასაზოვი",  # Eivaz Minasazovi
    "მოჰამმად მორთაზავი",  # Mohamed Mortazavi
    "აშოკ კუმარ",  # Ashok Kumar
    'ინკ"',  # INK
    "იანი მარიაბილდ Iსარ",  # Iani Mariabild I III
    "მოჰამმედ ჰასსან ა",  # Mohamed Hassan A
    "ბარნოვას რნი",  # Barnovas Rni
    "ბ",  # B
    "პაკისტანი",  # Pakistan
    "ვიკამსქეი დეკასტროსტრიტი",  # Vikamskei Decastrostreet
    "უნგრეთი",  # Hungary
    "შპს",  # LLC
    "შრი-ლანკა",  # Sri Lanka
    "მეცნიერების და",  # Department of Science
    "კომორის კუნძულები",  # Comoros
    "ფიჯი",  # Fiji
    "მორის პიერ დე პულპიკე დუ ალგუეტი",  # Moris Pier de Pulpike du Algueti
    "ტუნისი",  # Tunisia
    "ლაიპციგი",  # Liechtenstein
    "თაებეჰ როსთამი",  # Taebheh Rostami
    'სამცხეავტოგზა"',  # Samtskhetogza
    "დიდი ბრიტანეთი",  # Great Britain
    "მოზამბიკი",  # Mozambique
    "სამხ აფრიკის რესპუბლიკა",  # Republic of South Africa
    "მალაიზია",  # Malaysia
    "ათათურქის გამზ ილდიზის დ",  # Ataturk's Gaz Ildizi D
    "კასელი",  # Caselli
    "სართ",  # SA
    "აასი",  # Aasi
    "გამგებელი",  # Gamgebeli
    "სამინ ვოსსუგირად",  # Samin Vossugirad
    "ბულგარეთი",  # Bulgaria
    "სეიშელისკუნძულები",  # Seychelles
    "დიდი ბრიტანეთისა და",  # Great Britain and
    "საიედ მოჰამად ტაბატაბეი ნასაბ",  # Sayed Mohamed Tabatabei Nasab
    "ლესლიენ",  # Leslien
    "ყაზახეთი",  # Kazakhstan
    "ტაილანდი",  # Thailand
    "სამხრეთ",  # South
    "ვენესუელა",  # Venezuela
    "კოსტა-რიკა",  # Costa Rica
    #############################################
    "მაკედონია",  # Macedonia
    "მიანმარი",  # Myanmar
    "ფილიპინები",  # Philippines
    "სომხეთი",  # Somalia
    "აბდოლრეზა ებრაჰიმი",  # Abdulreza Ebrahimi
    "ბდაბ ოთახები",  # BDAB Houses
    "კოსტარიკა",  # Costa Rica
    "ფარშიდ კაშიგარ",  # Farshid Kashigar
    "მალავი",  # Malawi
    "ანგილია",  # Anguilla
    "ანდრე ჟირო",  # Andre Jiro
    "სულიმან ჰამიდ დ",  # Suliman Hamid D
    "ირანი (ისლამური რესპუბლიკა)",  # Iran (Islamic Republic of)
    "მეჰმეთ ბერათ ჩალიქ",  # Mehmet Berat Calik
    "ღოლამჰასან აბდოლინეჟადბაღმიშე",  # Golamhasan Abdulinejadbaghmishe
    "ნორვეგია",  # Norway
    "ტოგო",  # Togo
    "ისლანდია",  # Iceland
    "ოთახი  თაუერი   გეითვეი",  # House Tauri Gatevei
    "მონაკო",  # Monaco
    "ბაზრის მოედანი#",  # Market Square
    "სეიედ აბოლფაზლ გოლესტანეჰ",  # Sayed Abdolpazl Golestaneh
    "გვინეა",  # Guinea
    "რამინ იოსეფ ზადე ჯოკანდან",  # Ramin Joseph Zade Jokandan
    "ინქ",  # INK
    "ჩადი",  # Chad
    'ნიკორაკახეთი"',  # Nikorakakheti
    "მალი",  # Mali
    "მეჰდი დავარიან",  # Mehdi Davarian
    "სენტკრისტოფერი და ნ",  # Saint Christopher and N
    "სამხრეთ აფრიკა",  # South Africa
    "მაია ბოლღაშვილი",  # Maia Bolghashvili
    "საუნივერსიტეტო კლინიკანეიროქირურგიის ცენტრი",  # University Clinical Surgical Center
    "თინათინ თურქია ()",  # Tinatin Turkia
    "WCR DZ სტრენდი",  # WCR DZ Strand
    "ნინო",  # Nino
    "ზიალ პლაზა VVI სართ",  # Zial Plaza VVI SA
    "ერიტრეა",  # Eritrea
    "სირიის არაბთარესპუბლიკა",  # Syrian Arab Republic
    "დიდი",  # Big
    "ლის ალბერტოდამოტაპიტო #",  # Lis Albertodamotapito #
    "იემენი",  # Yemen
    "ლიბერია",  # Liberia
    "ბუღდაისოქაქ",  # Bugdaisokak
    "ნეპალი",  # Nepal
    "ალიასღარ ხანჯანიჯელოუდარ",  # Aliasghar Khanjanjeloudar
    "სეინტ კიტს და ნევისი",  # Saint Kitts and Nevis
    "როშანაკ რეზაიმეჰრ",  # Roshanak Rezaimer
    "პოლონეთი",  # Poland
    "ალი აბბას აბდ ალი შამხი",  # Ali Abbas Abd Ali Shamkhi
    "ყირგიზეთი",  # Kyrgyzstan
    "ლიტვა",  # Lithuania
    "საუდის არაბეთის სამეფო",  # Saudi Arabia
    'აფხაზინტერკონტი"',  # Apkhazinterkonti
    "ემად ედვარ ჰაბიბ აბდელმესსიჰ",  # Emad Edvar Habib Abdelmessih
    "სეიედმოჰამადრეზა ჰოსეინიბასთანი",  # Sayedmohammadreza Hosseinibastani
    "დელავერის შტატი",  # Delaware State
    "განა",  # Gana
    "მოჰამადბაღერ ნიკბახშთეჰრანი",  # Mohamedbager Nikbakhtehran
    "დე კასტროს ქ #",  # De Castro's Street #
    "ვირჯინიისკუნძულები (ბრიტ )",  # Virgin Islands (Brit)
    "საუდისარაბეთის სამეფო",  # Saudi Arabia
    "გამბია",  # Gambia
    "ირლანდია",  # Ireland
    "დიდი ბრიტანეთი (GBR)",  # Great Britain (GBR)
    "უზბეკეთი",  # Uzbekistan
    "არაბთა გაერთიანებული საამიროებ",  # United Arab Emirates
    "ჯავად ალი ა",  # Javad Ali A
    "მეისამ ჰეიდარი სოურეშჯანი",  # Meisam Heidar Soureshjani
    "კენია",  # Kenya
    #############################################
    "დე კასტროს ქუჩა",  # De Castro's Street
    "ემად ფარაგ იოაკიმ ბოტროს",  # Emad Farag Ioakim Botros
    "სადეღ თორქი ზადეჰ",  # Sadegh Torki Zadeh
    "გამგეობის თავმჯდომარის მოადგილე",  # Gamgeobis Tavmjdomari's Moaadgile
    "არაბთა გაერთიანებულისაემიროები",  # United Arab Emirates
    'ახალი სტილი"',  # New Style
    "აზარ მაჰმუდიალენჯარყი",  # Azar Mahmudialenjari
    "მოლდოვა",  # Moldova
    "შუმავსკას ქ",  # Shumavskas Street
    "ვირჯინიის",  # Virgin
    "თამარ ჯამარჯაშვილი",  # Tamar Jamarjashvili
    "საინფორმაციო ტექნოლოგიების მენეჯერი",  # Information Technologies Manager
    "ინკ",  # INK
    "ავთანდილ ნემსიწვერიძე",  # Avtandil Nemsitsveridze
    "შვეიცარია ()",  # Switzerland
    "ლატვია",  # Latvia
    "სირია",  # Syria
    "ალაა ეღბალბეჰბაჰანი",  # Alaa Egbalbehabhani
    "ენდოკრინული და გულფილტვის დაავადებების ცენტრიდიაკორი",  # Endocrinology and Cardiovascular Diseases Center
    "#",  # #
    "ბაჰრეინი",  # Bahrain
    "ლიბანი",  # Lebanon
    "მ",  # M
    "ბურუნდი",  # Burundi
    "ტაჯიკეთი",  # Tajikistan
    "თურქმენეთი",  # Turkmenistan
    "რიმ ისსამ აბდელაზიმ აჰმედ ელატტარ",  # Rim Issam Abdelazim Ahmed Elattar
    "გაბონი",  # Gabon
    "მარიამ ესმაილზადეჰ",  # Mariam Esmailzadeh
    'შავი ზღვის საერთაშორისოუნივერსიტეტი"',  # Black Sea International University
    "ამორე",  # Amore
    "ქ შკლოვი",  # K Sklov
    "ზურაბ პოლოლიკაშვილი",  # Zurab Pololikashvili
    "ირანი (ისლამური",  # Iran (Islamic
    "ქ  #",  # K #
    "მარშალის კუნძულები",  # Marshall Islands
    "კოტდივუარი",  # Cotdivuari
    "ჰანგთინთონ ბიჩ",  # Hangtinten Beach
    "ერაყი",  # Iraq
    "ტრინიტი ჰაუსი",  # Trinidad House
    "ანტრეი ბაგკნტასარიანი",  # Andrei Bagkntasariani
    "პარაგვაი",  # Paraguay
    "ბანგლადეში",  # Bangladesh
    "% SH",  # % SH
    "სეიშელის კუნძულები",  # Seychelles
    "არგენტინა",  # Argentina
    "ვაი ჩუნგ ჩან",  # Vai Chung Chan
    "იორდანია",  # Jordan
    "აჯელტაკისკუნძ სატრასტო კ",  # Ajeltakiskunz Satrasto K
    "სენტკიტსი დანევისი",  # Saint Kitts and Nevis
    "მობილია მალაზემელერი",  # Mobile Malazemeleri
    "ჩილე",  # Chile
    "დევიდ იჩბია",  # David Ichbia
    "რუსეთი",  # Russia
    "ალი იბრაჰიმ ემ",  # Ali Ebrahim Em
    "კრეიგმურ",  # Craigmur
    #############################################
    "კაიმანის კუნძულები",
    "სირიისარაბთა რესპუბლიკა",
    "ბადრიიეჰ იუნესი",
    "სამხრეთაფრიკა",
    "შპს აიდიესბორჯომი ბევერიჯიზ კომპანის საქართველოს",
    "სინგაპური",
    "რიადჰ აბდულაჰ მ",
    "გაერთიანებული სამეფო",
    "არაბთაგაერთიანებული საემიროები",
    "ლუქსემბურგის დიდი საჰერცოგო",
    "კორეის სახალხო დემოკრატიული რესპუბლიკა",
    "დიდიბრიტანეთისა და ჩრდილოეთ ირლანდიისგაერთიანებული სამეფო",
    "კამერუნი",
    "ზამბია",
    "ჰანაა მეგლაა ბოტროსაბდელმესსიჰ",
    "ალბერტ ვაჰბა მაკარ მათთა",
    "გიბრალტარი",
    'ნოვა ბილდინგ"',
    "ჩეხეთისრესპუბლიკა",
    "ნასერ ესკანდარი",
    "აჰმად აბდულრაჰმან ჰ",
    "თურქეთი ()",
    "საორგანიზაციო მდივანი",
    "ირანი (ისლამურირესპუბლიკა)",
    "ესტონეთი",
    "ისრაელისაქართველო",
    "კუნძული მენი",
    "სართული",
    "სირიის არაბთა",
    "ХТИ №",
    "როუდ თაუნი",
    "ნარგეს მაჰერ ისააქ კეჰერ",
    'სუხიშვილის უნივერსიტეტი" (ყოფილიშ პ ს',
    "აშშ",
    "ბელიზი",
    "კურასაო",
    "ბაჰრეინი მანამა",
    "ბელგია",
    "აიტეკე ბი ქ # VIსარ",
    "აფხაზეთი",
    "თავმჯდომარის მოადგილე",
    "ფერსტ აილენდ",
    "გამზ",
    "ისრაელი ()",
    "სალე ბეირამი",
    "ეთიოპია",
    "მარიამ ბებურიშვილი",
    "მალტა",
    "ლუდოვიკ ბელ",
    "კორეის სახალხო დემოკრატიული რე",
    "ბურკინა ფასო",
    "ლიეტუვა",
    "ჰანი გჰავიდელ",
    'დიდგორი"',
    "ესპანეთი",
    "ფრანსის დრეიკის გზატ",
    "ჰოლანდია",
    'ვიქტორია"',
    "ალირეზა აბჯადპურ",
    "სენტ-ვინსენტი და გრენადინები",
    "ჯოელი ტროშანი",
    "სენტკიტსი და ნევისი",
    "გერმანია",
    "ლიბიის არაბთა ჯამაჰირია",
    "დე კასტროს ქ",
    "s r o",
    "დიდიბრიტანეთისა და ჩრდილოეთირლანდიის გაერთიანებულისამეფო",
    "ეგვიპტე",
    'ალაფიაგრო"',
    "აბდულჰადი აბდულრაბალრასოლ მ",
    "სიამაკ პოურია",
    "მე სართ  #",
    "საბერძნეთი",
    "საფრანგეთი",
    "ბენქინგ დისტრიქტ",
    "მარი ეზზატ ფაიაზ მატიას",
    "სუბტროპიკული კულტურებისა და ჩაისმრეწველობის ინსტიტუტი",
    "ვიეტნამი",
    #############################################
    "თურქეთი",
    "მარკ  სქვეა",
    "გოლამრეზა დამერჯი მაჰმუდი",
    "მდივანი",
    "ჩეხეთი",
    "ჩადზიპავლოუ",
    "დიდი ბრიტანეთისა და ჩრდილოეთ ირლანდიის",
    "მილანი",
    "ჰამიდ ნაფარ",
    "ბენინი",
    "მოჰამმად რეზა შირაზი",
    "აზარბადეგან",
    "უგანდა",
    "ვირჯინიის კუნძულები(ბრიტ )",
    "თავმჯდომარის პირველი მოადგილე",
    "ელიზ კოლცზ",
    "არაბთა",
    "კანადა",
    "ვიხამს ქეი",
    "სალეჰ ჰამად ეი",
    "გვინეა-ბისაუ",
    "გარემოს დამცველი",
    "ზიმბაბვე",
    "სირიის არაბთა რესპუბლიკა",
    "ვირჯინიის კუნძულები (ბრიტ.)",
    'პოიკერ და ნებელ"',
    "სომალი",
    "(საქართველო)",
    "მოჰამმედ აჰმედ ჯ",
    "რუსეთის",
    "რობერტ",
    "დანია",
    "შრილანკა",
    'ინაკო"',
    "პროკურისტი",
    "სულემან საუდ ს",
    "ჰოლსველ როუდ ლონდონი",
    "ლიბია",
    "ამერიკის სამოა",
    "ეი ფი",
    "ვანუატუ",
    "უკრაინა ()",
    "პალაუ",
    "აბდულრაჰმან მაჰმუდ ჰ",
    "სენტ-კიტსი და ნევისი",
    "ნიგერი",
    "დიდი ბრიტანეთისა და ჩრდილოეთ",
    "დომინიკელთა რესპუბლიკა",
    "აიტეკები VI სართ",
    "ვირჯინიის კუნძულები (ბრიტ )",
    "მოჰამმად ხოდაიარი",
    "მევქიის რი თერმე",
    "მაჰერ ლაბიბ საად მოავად",
    "რუმინეთი",
    "ხორვატია",
    "დ უმირზაკრგპ ტერიტორია",
    "კოსოვო",
    "არაშ შაჰფარი",
    "ჩინეთი",
    "დან",
    "ნაუოზის",
    "მურათ იღით ჩალიქ",
    "კორეის რესპუბლიკა",
}


def crawl_row(context: Context, row: list) -> None:
    id = row.pop("id")
    name = row.pop("name")
    legal_form = row.pop("legal_form")
    reg_date = row.pop("registration_date")
    email = row.pop("email")
    director = row.pop("director")
    partner = row.pop("partner")

    if legal_form not in LEGAL_FORMS:
        context.log.warning(f"Unknown legal form: {legal_form}")
        return

    if any(org in legal_form for org in ORGS):
        legal_form = "Organization"
    else:
        legal_form = "Company"

    entity = context.make(legal_form)
    entity.id = context.make_id(id, name, reg_date)
    entity.add("name", name)
    entity.add("classification", legal_form)
    entity.add("incorporationDate", reg_date)
    entity.add("address", row.pop("address"))
    entity.add("status", row.pop("status"))
    emails = email.replace(" ", "").strip()
    for email in h.multi_split(emails, [";", ","]):
        email_clean = registry.email.clean(email)
        if email_clean is not None:
            entity.add("email", email)
    context.emit(entity)

    if director != "NULL":
        emit_rel(
            context,
            "Directorship",
            row,
            director,
            entity,
            "director_id",
            "director_citizenship",
            "director_start_date",
            "director",
        )
        context.audit_data(
            row,
            ignore=[
                "partner_id",
                "partner_citizenship",
                "partner_start_date",
                "partner_share",
            ],
        )
    if partner != "NULL":
        emit_rel(
            context,
            "Ownership",
            row,
            partner,
            entity,
            "partner_id",
            "partner_citizenship",
            "partner_start_date",
            "owner",
        )
        context.audit_data(
            row,
            ignore=[
                "director_id",
                "director_citizenship",
                "director_start_date",
            ],
        )


def emit_rel(
    context: Context,
    type_label: str,
    row: list,
    name: str,
    entity,
    id_key: str,
    citizenship_key: str,
    start_date_key: str,
    relationship_type: str,
):
    """Generalized function to process a director or partner."""
    person_id = row.pop(id_key)
    person_citizenship = row.pop(citizenship_key)
    person_start_date = row.pop(start_date_key)

    person = context.make("Person")
    person.id = context.make_id(name, person_id)
    person.add("name", name)
    if person_citizenship != "NULL":
        for citizenship in h.multi_split(person_citizenship, [","]):
            person.add("citizenship", citizenship)
    context.emit(person)

    relationship = context.make(type_label)
    relationship.id = context.make_id(person.id, relationship_type, entity.id)
    if person_start_date != "NULL":
        h.apply_date(relationship, "startDate", person_start_date)
    relationship.add(relationship_type, person)
    relationship.add(
        "organization" if relationship_type == "director" else "asset", entity
    )

    if relationship_type == "owner":
        relationship.add("percentage", row.pop("partner_share"))

    context.emit(relationship)


def crawl(context: Context) -> None:
    local_path = context.get_resource_path("companyinfo_v3.csv")
    for blob in list_internal_data("ge_ti_companies/"):
        if not blob.endswith(".csv"):
            continue

        fetch_internal_data(blob, local_path)
        context.log.info("Parsing: %s" % blob)

        with open(local_path, "r", encoding="utf-8") as fh:
            reader = csv.reader(fh, delimiter=";")
            original_headers = next(reader)

            # Translate headers to English
            header_mapping = [
                context.lookup_value("columns", collapse_spaces(cell))
                for cell in original_headers
            ]
            if len(header_mapping) != len(original_headers):
                context.log.warning("Mismatch between headers and row length.")
                return
            # Reset file pointer and skip original header
            fh.seek(0)
            next(fh)

            # Use DictReader with mapped headers
            dict_reader = csv.DictReader(fh, fieldnames=header_mapping, delimiter=";")
            for index, row in enumerate(dict_reader):
                crawl_row(context, row)
                # if index >= 100000:
                #     break
