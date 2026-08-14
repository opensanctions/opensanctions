PYTHON ?= python

all: parse validate

# One line per framework act; the consolidated version passed to each parse
# script matches the `consolidation:` lookup pin in eu_journal_sanctions.yml.
parse:
	$(PYTHON) scripts/parse_32014R0833.py 02014R0833-20260717

validate:
	$(PYTHON) validate_csv.py

.PHONY: all parse validate
