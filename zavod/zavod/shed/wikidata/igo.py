from dataclasses import dataclass, field


@dataclass(frozen=True)
class IntlOrg:
    """Registry entry for an international body whose positions are exempt
    from the country gate in `wikidata_position`."""

    name: str
    country: str = "zz"
    topics: frozenset[str] = field(default=frozenset({"gov.igo"}))


SPORT = frozenset({"poi"})

# International bodies whose positions OpenSanctions wants despite having no
# national jurisdiction. A position joins this path when its P2389
# ("organization directed by the office") or P361 ("part of") points at a
# registry org: it gets the entry's country code and topics, its own
# country/jurisdiction claims are ignored (they hold headquarters countries
# or the org itself more often than anything meaningful), and it enters the
# position review UI as undecided rather than being silently dropped.
#
# Seeded from the 2026-08-05 census of P39-used positions with org links
# (wikidata workbench, tasks/international_positions.md). Orgs are few and
# stable; extend this list rather than special-casing positions.
INTL_ORGS: dict[str, IntlOrg] = {
    # United Nations system:
    "Q1065": IntlOrg("United Nations", country="un"),
    "Q47423": IntlOrg("United Nations General Assembly", country="un"),
    "Q37470": IntlOrg("United Nations Security Council", country="un"),
    "Q220563": IntlOrg("United Nations Secretariat", country="un"),
    "Q170075": IntlOrg("United Nations Economic and Social Council", country="un"),
    "Q205650": IntlOrg("United Nations Human Rights Council", country="un"),
    "Q656812": IntlOrg(
        "Office of the High Commissioner for Human Rights", country="un"
    ),
    "Q132551": IntlOrg("United Nations High Commissioner for Refugees", country="un"),
    "Q846656": IntlOrg("United Nations Relief and Works Agency", country="un"),
    "Q1065854": IntlOrg(
        "UN Office for the Coordination of Humanitarian Affairs", country="un"
    ),
    "Q161718": IntlOrg("United Nations Development Programme", country="un"),
    "Q740308": IntlOrg("UNICEF", country="un"),
    "Q641576": IntlOrg("UN Women", country="un"),
    "Q2531088": IntlOrg("United Nations Office for Project Services", country="un"),
    "Q32874": IntlOrg(
        "UN Economic Commission for Latin America and the Caribbean", country="un"
    ),
    "Q3708827": IntlOrg(
        "United Nations Department of Global Communications", country="un"
    ),
    "Q135418656": IntlOrg(
        "UN Office for Digital and Emerging Technologies", country="un"
    ),
    "Q7888477": IntlOrg(
        "United Nations Office for West Africa and the Sahel", country="un"
    ),
    "Q160805": IntlOrg("United Nations Interim Force in Lebanon", country="un"),
    "Q2671637": IntlOrg("UN Department of Economic and Social Affairs", country="un"),
    # WFP has no leadership position items on Wikidata yet (2026-08-05);
    # registered so they enroll as soon as someone creates and links them.
    "Q204344": IntlOrg("World Food Programme", country="un"),
    "Q636107": IntlOrg("United Nations Office on Drugs and Crime", country="un"),
    # UN specialized agencies (the Bretton Woods institutions are deliberately
    # "zz": they operate as global bodies, not as UN organs):
    "Q7817": IntlOrg("World Health Organization", country="un"),
    "Q7809": IntlOrg("UNESCO", country="un"),
    "Q82151": IntlOrg("Food and Agriculture Organization", country="un"),
    "Q54129": IntlOrg("International Labour Organization", country="un"),
    "Q201054": IntlOrg("International Maritime Organization", country="un"),
    "Q376150": IntlOrg("International Telecommunication Union", country="un"),
    "Q170424": IntlOrg("World Meteorological Organization", country="un"),
    "Q177773": IntlOrg("World Intellectual Property Organization", country="un"),
    "Q689859": IntlOrg("International Fund for Agricultural Development", country="un"),
    "Q386120": IntlOrg(
        "United Nations Industrial Development Organization", country="un"
    ),
    "Q61923": IntlOrg("UN Tourism", country="un"),
    "Q7804": IntlOrg("International Monetary Fund"),
    "Q320863": IntlOrg("World Bank Group"),
    # International courts:
    "Q7801": IntlOrg("International Court of Justice"),
    "Q47488": IntlOrg("International Criminal Court"),
    "Q122880": IntlOrg("European Court of Human Rights"),
    "Q3001122": IntlOrg("Eastern Caribbean Supreme Court"),
    # European Union institutions ("eu" is a rigour pseudo-territory, so many
    # of their positions already resolve via P1001; the registry makes org-only
    # links discoverable too):
    "Q458": IntlOrg("European Union", country="eu"),
    "Q8889": IntlOrg("European Parliament", country="eu"),
    "Q10749015": IntlOrg("Bureau of the European Parliament", country="eu"),
    "Q8880": IntlOrg("European Commission", country="eu"),
    "Q1501921": IntlOrg("Secretariat-General of the European Commission", country="eu"),
    "Q2983826": IntlOrg("College of Commissioners", country="eu"),
    "Q8886": IntlOrg("European Council", country="eu"),
    "Q8896": IntlOrg("Council of the European Union", country="eu"),
    "Q2067116": IntlOrg("General Secretariat of the Council of the EU", country="eu"),
    "Q973809": IntlOrg("Foreign Affairs Council", country="eu"),
    "Q149964": IntlOrg("Eurogroup", country="eu"),
    "Q1518827": IntlOrg("European Court of Justice", country="eu"),
    "Q4951": IntlOrg("Court of Justice of the European Union", country="eu"),
    "Q8900": IntlOrg("European Court of Auditors", country="eu"),
    "Q8901": IntlOrg("European Central Bank", country="eu"),
    "Q657898": IntlOrg("European Systemic Risk Board", country="eu"),
    "Q220893": IntlOrg("European Ombudsman", country="eu"),
    "Q672941": IntlOrg("European External Action Service", country="eu"),
    # The generic class item for EU diplomatic missions; heads of delegation
    # (EU ambassadors) link their position to it via P361:
    "Q130417640": IntlOrg("Delegation of the European Union", country="eu"),
    "Q4398720": IntlOrg("Secretariat of the European Parliament", country="eu"),
    "Q205203": IntlOrg("European Committee of the Regions", country="eu"),
    "Q331024": IntlOrg("European Economic and Social Committee", country="eu"),
    "Q1134173": IntlOrg("European Defence Agency", country="eu"),
    "Q5413070": IntlOrg("European Public Prosecutor's Office", country="eu"),
    "Q516521": IntlOrg("European Food Safety Authority", country="eu"),
    "Q192247": IntlOrg("European Investment Bank", country="eu"),
    "Q1934935": IntlOrg("European Union Military Committee", country="eu"),
    "Q25352258": IntlOrg("Economic and Financial Committee", country="eu"),
    "Q135841125": IntlOrg("European Artificial Intelligence Office", country="eu"),
    "Q1501753": IntlOrg("Directorate-General for Environment", country="eu"),
    "Q5280593": IntlOrg("Directorate-General for Internal Market", country="eu"),
    "Q5280596": IntlOrg("Directorate-General for Digital Services", country="eu"),
    "Q5280604": IntlOrg("Directorate-General for Regional Policy", country="eu"),
    "Q596757": IntlOrg("Directorate-General for Health", country="eu"),
    "Q82664954": IntlOrg("Directorate-General for Defence Industry", country="eu"),
    # Union agencies, authorities and joint undertakings:
    "Q130146": IntlOrg("European Medicines Agency", country="eu"),
    "Q21786841": IntlOrg("European Border and Coast Guard Agency", country="eu"),
    "Q207203": IntlOrg("Europol", country="eu"),
    "Q276255": IntlOrg("European Union Aviation Safety Agency", country="eu"),
    "Q48798": IntlOrg("European Chemicals Agency", country="eu"),
    "Q632988": IntlOrg("European Environment Agency", country="eu"),
    "Q1377194": IntlOrg("European Union Agency for Cybersecurity", country="eu"),
    "Q1377256": IntlOrg("European Union Drugs Agency", country="eu"),
    "Q1377490": IntlOrg(
        "European Union Agency for Law Enforcement Training", country="eu"
    ),
    "Q1377549": IntlOrg(
        "European Foundation for the Improvement of Living and Working Conditions",
        country="eu",
    ),
    "Q116214": IntlOrg(
        "European Centre for the Development of Professional Training", country="eu"
    ),
    "Q201372": IntlOrg("European Fisheries Control Agency", country="eu"),
    "Q4002697": IntlOrg("European Union Agency for Asylum", country="eu"),
    "Q4440501": IntlOrg(
        "European Union Agency for the Operational Management of Large-Scale IT Systems in the Area of Freedom, Security and Justice",
        country="eu",
    ),
    "Q55610347": IntlOrg("European Union Agency for the Space Programme", country="eu"),
    "Q56296301": IntlOrg("European Labour Authority", country="eu"),
    "Q571118": IntlOrg("European Agency for Safety and Health at Work", country="eu"),
    "Q792534": IntlOrg("European Union Intellectual Property Office", country="eu"),
    "Q918501": IntlOrg(
        "European Centre for Disease Prevention and Control", country="eu"
    ),
    "Q922693": IntlOrg("European Maritime Safety Agency", country="eu"),
    "Q369273": IntlOrg("European Union Agency for Railways", country="eu"),
    "Q372042": IntlOrg("European Training Foundation", country="eu"),
    "Q392883": IntlOrg(
        "European Union Agency for the Cooperation of Energy Regulators", country="eu"
    ),
    "Q48718": IntlOrg("European Institute for Gender Equality", country="eu"),
    "Q48790": IntlOrg("European Union Agency for Fundamental Rights", country="eu"),
    "Q993120": IntlOrg("European Institute of Innovation and Technology", country="eu"),
    "Q1388519": IntlOrg("European Union Satellite Centre", country="eu"),
    "Q289256": IntlOrg("European Union Institute for Security Studies", country="eu"),
    "Q333743": IntlOrg(
        "Translation Centre for the Bodies of the European Union", country="eu"
    ),
    "Q107658738": IntlOrg("Agency for Support for BEREC", country="eu"),
    "Q107658626": IntlOrg("European Cybersecurity Competence Centre", country="eu"),
    "Q138781968": IntlOrg("EU Customs Authority", country="eu"),
    "Q1501010": IntlOrg("Community Plant Variety Office", country="eu"),
    "Q769112": IntlOrg("European Securities and Markets Authority", country="eu"),
    "Q769548": IntlOrg("European Banking Authority", country="eu"),
    "Q2116004": IntlOrg(
        "European Insurance and Occupational Pensions Authority", country="eu"
    ),
    "Q124795462": IntlOrg("Anti-Money Laundering Authority", country="eu"),
    "Q19516507": IntlOrg("Single Resolution Board", country="eu"),
    "Q26877724": IntlOrg("European Data Protection Board", country="eu"),
    "Q1377836": IntlOrg("European Research Council", country="eu"),
    "Q3606472": IntlOrg("European Research Executive Agency", country="eu"),
    "Q106818821": IntlOrg(
        "European Climate, Infrastructure and Environment Executive Agency",
        country="eu",
    ),
    "Q1383639": IntlOrg(
        "European Education and Culture Executive Agency", country="eu"
    ),
    "Q734374": IntlOrg(
        "European Innovation Council and SMEs Executive Agency", country="eu"
    ),
    "Q60983149": IntlOrg(
        "European High-Performance Computing Joint Undertaking", country="eu"
    ),
    "Q133884630": IntlOrg("Clean Hydrogen Joint Undertaking", country="eu"),
    "Q3055255": IntlOrg("SESAR Joint Undertaking", country="eu"),
    "Q5510278": IntlOrg("Fusion for Energy", country="eu"),
    # Council of Europe (not the EU):
    "Q8908": IntlOrg("Council of Europe"),
    "Q939743": IntlOrg("Parliamentary Assembly of the Council of Europe"),
    "Q1251615": IntlOrg("Congress of Local and Regional Authorities"),
    "Q2735723": IntlOrg("Committee of Ministers of the Council of Europe"),
    # Other intergovernmental organizations:
    "Q7184": IntlOrg("NATO"),
    "Q944947": IntlOrg("North Atlantic Council"),
    "Q1959817": IntlOrg("NATO Military Committee"),
    "Q1432908": IntlOrg("Supreme Headquarters Allied Powers Europe"),
    "Q55858714": IntlOrg("Allied Command Operations"),
    "Q2001035": IntlOrg("Allied Command Transformation"),
    "Q13417611": IntlOrg("NATO Standardization Agency"),
    "Q81299": IntlOrg("Organization for Security and Co-operation in Europe"),
    "Q8475": IntlOrg("Interpol"),
    "Q41550": IntlOrg("OECD"),
    "Q7825": IntlOrg("World Trade Organization"),
    "Q194284": IntlOrg("General Agreement on Tariffs and Trade"),
    "Q7795": IntlOrg("OPEC"),
    "Q41984": IntlOrg("International Atomic Energy Agency"),
    "Q7159": IntlOrg("African Union"),
    "Q2362881": IntlOrg("African Union Commission"),
    "Q191703": IntlOrg("Organisation of African Unity"),
    "Q193272": IntlOrg("Economic Community of West African States"),
    "Q337456": IntlOrg("East African Community"),
    "Q5327657": IntlOrg("East African Legislative Assembly"),
    "Q1115631": IntlOrg("Indian Ocean Commission"),
    "Q294278": IntlOrg("Organisation of African, Caribbean and Pacific States"),
    "Q7172": IntlOrg("League of Arab States"),
    "Q217172": IntlOrg("Gulf Cooperation Council"),
    "Q47543": IntlOrg("Organisation of Islamic Cooperation"),
    "Q111169280": IntlOrg("Islamic Organisation for Food Security"),
    "Q123759": IntlOrg("Organization of American States"),
    "Q205995": IntlOrg("Caribbean Community"),
    "Q1153087": IntlOrg("Inter-American Development Bank"),
    "Q4230": IntlOrg("Union of South American Nations"),
    "Q9075403": IntlOrg("Ibero-American General Secretariat"),
    "Q83201": IntlOrg("Non-Aligned Movement"),
    "Q182379": IntlOrg("Nordic Council of Ministers"),
    "Q488981": IntlOrg("European Bank for Reconstruction and Development"),
    "Q2883427": IntlOrg("West African Development Bank"),
    "Q1010514": IntlOrg("Bureau of International Expositions"),
    "Q1130098": IntlOrg("European Stability Mechanism"),
    "Q2020649": IntlOrg("Nordic Investment Bank"),
    "Q611107": IntlOrg("Council of Europe Development Bank"),
    "Q2061083": IntlOrg("Black Sea Trade and Development Bank"),
    "Q604488": IntlOrg("Common Fund for Commodities"),
    "Q826700": IntlOrg("International Energy Agency"),
    "Q842490": IntlOrg("Organisation for the Prohibition of Chemical Weapons"),
    "Q369760": IntlOrg("Nuclear Energy Agency"),
    "Q1058594": IntlOrg("World Organisation for Animal Health"),
    "Q1317185": IntlOrg("International Organisation of Vine and Wine"),
    "Q1967903": IntlOrg("European and Mediterranean Plant Protection Organization"),
    "Q789769": IntlOrg("Council of the Baltic Sea States"),
    "Q134102": IntlOrg("Organisation internationale de la Francophonie"),
    "Q605326": IntlOrg("World Customs Organization"),
    "Q1192641": IntlOrg("UNIDROIT"),
    "Q13116": IntlOrg("Benelux"),
    "Q192177": IntlOrg("Union for the Mediterranean"),
    "Q392805": IntlOrg("Central European Initiative"),
    "Q421023": IntlOrg("Organization of Ibero-American States"),
    "Q1045198": IntlOrg("Permanent Court of Arbitration"),
    "Q152299": IntlOrg("Dutch Language Union"),
    "Q30104861": IntlOrg("European Schools"),
    "Q182500": IntlOrg("Community of Portuguese Language Countries"),
    "Q1563503": IntlOrg("HELCOM"),
    "Q1666088": IntlOrg(
        "Intergovernmental Science-Policy Platform on Biodiversity and Ecosystem Services"
    ),
    "Q47537350": IntlOrg("European Telecommunications Satellite Organization"),
    "Q840267": IntlOrg(
        "International Commission for the Conservation of Atlantic Tunas"
    ),
    "Q821623": IntlOrg("EU–LAC Foundation"),
    "Q356694": IntlOrg("Comprehensive Nuclear-Test-Ban Treaty Organization"),
    # Treaty-based scientific organizations:
    "Q42944": IntlOrg("CERN"),
    "Q42262": IntlOrg("European Space Agency"),
    "Q151991": IntlOrg("European Southern Observatory"),
    "Q692163": IntlOrg("EUMETSAT"),
    "Q695267": IntlOrg("European Molecular Biology Laboratory"),
    "Q229478": IntlOrg("International Bureau of Weights and Measures"),
    # International sports bodies — kept out of gov.* topics; their officials
    # are persons of interest, not government officials:
    "Q253414": IntlOrg("FIFA", topics=SPORT),
    "Q40970": IntlOrg("International Olympic Committee", topics=SPORT),
    "Q47472719": IntlOrg("IOC Ethics Commission", topics=SPORT),
    "Q35572": IntlOrg("UEFA", topics=SPORT),
    "Q46199": IntlOrg("International Basketball Federation", topics=SPORT),
    "Q58733": IntlOrg("CONMEBOL", topics=SPORT),
    "Q1158": IntlOrg("World Athletics", topics=SPORT),
    "Q684885": IntlOrg("World Rowing", topics=SPORT),
    "Q708793": IntlOrg("International Shooting Sport Federation", topics=SPORT),
}
