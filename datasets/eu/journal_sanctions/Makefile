PYTHON ?= python

all: parse validate

# One line per framework act; the consolidated version passed to each parse
# script matches the `consolidation:` lookup pin in eu_journal_sanctions.yml.
parse:
	$(PYTHON) scripts/parse_32006R0765.py 02006R0765-20260424
	$(PYTHON) scripts/parse_32014R0833.py 02014R0833-20260717
	$(PYTHON) scripts/parse_32016R0044.py 02016R0044-20260616
	$(PYTHON) scripts/parse_32017R1770.py 02017R1770-20251203
	$(PYTHON) scripts/parse_32020R1998.py 02020R1998-20260528
	$(PYTHON) scripts/parse_32024R1485.py 02024R1485-20260713
	$(PYTHON) scripts/parse_32024R2642.py 02024R2642-20260713

validate:
	$(PYTHON) validate_csv.py

.PHONY: all parse validate
