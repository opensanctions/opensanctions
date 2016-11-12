Ad-hoc method to generate list of politically-exposed persons from
Wikipedia category pages. This is not meant to produce high-quality
data, but rather give a lead list for further investigation and 
data cross-referencing.


## Notes

https://tools.wmflabs.org/wikidata-exports/miga/#_item=2222
https://tools.wmflabs.org/wikidata-exports/miga/#_item=2


Political parties:

SELECT ?subj ?label ?prop ?val ?lang WHERE {
   ?subj wdt:P31 wd:Q7278 .
   ?subj ?prop ?val .
   ?subj rdfs:label ?label filter (lang(?label) = "en")
   BIND (lang(?val) AS ?lang)
}


relevant classes:

legal person -- Id: Q19817303
person -- Id: Q215627
legal person -- Id: Q19817303
public figure -- Id: Q662729
human -- Id: Q5

minister -- Q83307


https://tools.wmflabs.org/wikidata-exports/miga/#_cat=Classes/All%20superclasses=human


SELECT ?subj ?label ?prop ?val ?lang WHERE {
   ?subj wdt:P31 wd:Q5 .
   ?subj ?prop ?val .
   ?subj rdfs:label ?label filter (lang(?label) = "en")
   BIND (lang(?val) AS ?lang)
}

