JD: what if we make it easier to find these in the review UI? currently we have a page listing reviews for a dataset with free text search. Maybe we can make the search optionally specific to a particular field, e.g. only search in the extracted_data field, and toggle showing source_value, original_extraction, extracted_data, also an option to filter for only reviews where original and final extracted differ? then there's no jq step - we can identify and fix inconsistencies in the UI.

LLM:
On the UI idea: yes, and it's a small change. Everything you describe lands in one query and one component.

Where it goes. The list page calls getExtractionEntries(dataset, search) in ui/lib/db.ts, which already ORs an ILIKE over source_value, source_url, modified_by and the two JSON columns cast to text. The page renders one table with a 100-character snippet of original_extraction and nothing else from the data. So the change is:

- Scoped search. A scope URL param (all, source, original, extracted) that picks which of the existing five ILIKE clauses to keep. Ten lines in the query builder, a select box in SearchInput.
- Differs filter. A differs param adding CAST(original_extraction AS jsonb) <> CAST(extracted_data AS jsonb). The columns are json, which has no equality operator, so the jsonb cast is required. On the SECO dataset that drops the list from 1060 rows to 201. Worth checking whether the UI's getDb also has to work against SQLite for local dev, since the cast differs there.
- Toggle columns. Checkboxes for source, original, extracted. Rendering three full JSON blobs for 1000 rows is heavy, so I'd default them off and pair them with the differs filter.

One addition I'd make. Instead of, or as well as, showing the raw original and extracted JSON, compute a diff column server-side per row: only the paths that changed, e.g. related_entities: −"Hottak tribe" or simple_values: registrationNumber→ogrnCode 1077757722206. That is exactly the signal for spotting inconsistency, and it stays legible at 200 rows where two full JSON columns don't. It's a short TypeScript function over the two objects, dataset-agnostic.

How it replaces the jq step. The category groupings I was going to bake into a worklist become searches: scope=source, differs=on, query "listed as" shows every "X, listed as Y (QDe…)" case with what each reviewer did to it, side by side. Same for "Associated entit", "tribe", "Ministry", "Rank:". The reviewer column is already there for attribution, and the key links straight to the edit page.

What it doesn't cover. Inconsistency inside the unedited set, like the 46-versus-35 split on countries in position titles, or the accepted review whose tax number isn't in the text. Those have original equal to extracted, so the differs filter hides them. For those you'd search scope=extracted for a value and eyeball, or run the harness. That's fine; the edited set is where the policy disagreements concentrate.

I'd estimate it at one session: query params and SQL, the diff helper with a unit test in db.test.ts, three controls and three optional columns in the list page. Say if you want me to build it.
