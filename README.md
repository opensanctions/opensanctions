# OpenNames

A consolidated, public database of persons with political or criminal relevance. The source data for this repository is a variety of sanctions lists, political databases and related information sources. 

## Installation


### Environment variables

* ``OPENNAMES_SOURCE_DATA``, the path in which source data will be stored.
* ``OPENNAMES_JSON_DATA``, output path for parsers, containing the generated JSON.

## Data Sources

Possible data sources for this project (only a few are included thus far):

* **US** Treasury/OFAC, [Specially Designated Nationals](https://www.treasury.gov/resource-center/sanctions/SDN-List/Pages/default.aspx), [Consolidated List](https://www.treasury.gov/resource-center/sanctions/SDN-List/Pages/consolidated.aspx).
* **US** State Dept., designated [Foreign Terrorist Organisations](http://www.state.gov/j/ct/rls/other/des/123085.htm).
* **US** Terrorist [Exclusion List](http://www.state.gov/j/ct/rls/other/des/123086.htm)
* **US** BIS/Commerce [Denied Persons](https://www.bis.doc.gov/index.php/policy-guidance/lists-of-parties-of-concern/denied-persons-list).
* **US** State, Iran [Sanctioned Entities List](http://www.state.gov/e/eb/tfs/spi/iran/entities/index.htm).
* **US** State [Diplomatic List](http://www.state.gov/s/cpr/rls/dpl/243893.htm#azerbaijan).
* **US** State, [Non-Proliferation Sanctions](http://m.state.gov/md226423.htm).
* **US** CIA [World Leaders](https://www.cia.gov/library/publications/resources/world-leaders-1/AF.html).
* **GB** HMT [Sanctions Consolidated List](http://hmt-sanctions.s3.amazonaws.com/sanctionsconlist.htm), Reference on [gov.uk](https://www.gov.uk/government/publications/financial-sanctions-consolidated-list-of-targets/consolidated-list-of-targets) and [DGU Financial Sanctions](https://data.gov.uk/dataset/financialsanctions).
* **GB** Insolvency, [Disqualified Directors](https://www.insolvencydirect.bis.gov.uk/IESdatabase/viewdirectorsummary-new.asp).
* **EU** [Ukraine Sanctions](http://eur-lex.europa.eu/legal-content/EN/TXT/?uri=uriserv:OJ.L_.2014.137.01.0003.01.ENG).
* **EU** EEAS [Consolidated Lists](http://eeas.europa.eu/cfsp/sanctions/consol-list/index_en.htm).
* **UA** Sanctions [Blacklist](http://www.sdfm.gov.ua/content/file/Site_docs/Black_list/zBlackListFull.xml).
* **AU** Australia Dept. of Foreign Affairs and Trade [Regulation 8 Consolidated List](http://dfat.gov.au/international-relations/security/sanctions/Documents/regulation8_consolidated.xls).
* **IS** [Terrorist List](http://www.mod.gov.il/Defence-and-Security/Fighting_terrorism/Pages/default.aspx), [xls](http://www.mod.gov.il/Defence-and-Security/Fighting_terrorism/Documents/terror_yahid%20-%2011-5-2015.xls).
* **CH** SECO [Sanctions List](http://www.seco.admin.ch/themen/00513/00620/index.html?lang=en).
* **UN** Security Council [Consolidated List](https://www.un.org/sc/suborg/en/sanctions/un-sc-consolidated-list).
* World Bank Procurement [Debarred vendors](http://web.worldbank.org/external/default/main?contentMDK=64069844&menuPK=116730&pagePK=64148989&piPK=64148984&querycontentMDK=64069700&theSitePK=84266).
* **RU** [Guantanamo List](http://sputniknews.com/voiceofrussia/2013_04_13/Russia-s-Guantanamo-List-officially-published/).

### Less structured sources

* Wikipedia [List of Ukraine Sanctions](https://en.wikipedia.org/wiki/List_of_individuals_sanctioned_during_the_Ukrainian_crisis).
* [Rulers.org](http://rulers.org/) and [World Statesmen](http://worldstatesmen.org/)
* [Archigos](http://privatewww.essex.ac.uk/~ksg/archigos.html): A Database of Political Leaders.
* [Biografías Líderes Políticos](http://www.cidob.org/en/biografias_lideres_politicos_only_in_spanish) (Only in spanish)
* Leahy [vetting requests](https://en.wikipedia.org/wiki/Leahy_Law). 
* Aylas [List Directory](http://aylias.com/list/).
* [MrWatchLists](http://mrwatchlist.com/watchlists/).
* FBI [Most Wanted](http://www.fbi.gov/wanted)
* BKA [Fahnungen](http://www.bka.de/nn_205924/DE/Fahndungen/fahndungen__node.html?__nnn=true)
* SAPS [Most Wanted](http://www.saps.gov.za/crimestop/wanted/list.php)
* Bundesanzeiger [sources](http://217.17.30.74/SubDl/index.jsp?user=SLamtsblatt&pass=SLamtsblatt&path=ReadMe-Vorlage1en.pdf).
* [EU Who is Who](https://transparencycamp.eu/2016/04/13/who-is-who-in-eu-institutions/)


# License

The MIT License (MIT)

Copyright (c) 2015-2016 Friedrich Lindenberg

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
