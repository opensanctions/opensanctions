import requests
import json

URL = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
QUERY = """

SELECT ?subj ?label ?prop ?val ?lang WHERE {
   ?subj wdt:P31 wd:Q5 .
   ?subj ?prop ?val .
   ?subj rdfs:label ?label filter (lang(?label) = "en")
   BIND (lang(?val) AS ?lang)
} LIMIT 5
"""

res = requests.get(URL, data={'query': QUERY}, headers={
    'Accept': 'application/sparql-results+json'
})
data = json.loads(res.content)
data = data.get('results', {}).get('bindings', [])

for item in data:
    print item

