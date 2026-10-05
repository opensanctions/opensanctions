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
    # Treaty-based scientific organizations:
    "Q42944": IntlOrg("CERN"),
    "Q42262": IntlOrg("European Space Agency"),
    "Q151991": IntlOrg("European Southern Observatory"),
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
