## Debugging tools

When debugging behavior of the helpers in this directory, two standalone
CLIs in `contrib/wikidata/` are often useful:

* `contrib/wikidata/wd_item_debug.py` — for a single Wikidata QID, runs the
  entity-building helpers in this directory (`wikidata_basic_human`,
  `wikidata_position`, `wikidata_occupancy`) the same way the
  `_wikidata/peps` crawler does and prints the resulting statements in
  pack-CSV format. Use it to see why a specific person/position is (or
  is not) being emitted, or what their statements look like, without
  running the full crawler.

* `contrib/wikidata/wd_categories_paths.py` — walks a Wikipedia category
  tree downwards to find which nested categories link a given QID to a
  starting category. Useful for understanding which specific category
  caused a person to enter the candidate set fed to the helpers here,
  i.e. why the `wd_categories` dataset picked them up.

Together they cover the two questions you usually want to answer when
debugging the PEPs Wikidata pipeline: "why did this person get
considered?" (`wd_categories_paths.py`) and "what did our helpers do
with them once they were considered?" (`wd_item_debug.py`).
