PYTHON ?= python

all: parse validate

# One line per framework act; the consolidated version passed to each parse
# script matches the `consolidation:` lookup pin in eu_journal_sanctions.yml.
parse:
	$(PYTHON) scripts/parse_32001R2580.py 02001R2580-20260227
	$(PYTHON) scripts/parse_32002R0881.py 02002R0881-20260721
	$(PYTHON) scripts/parse_32006R0765.py 02006R0765-20260424
	$(PYTHON) scripts/parse_32011D0173.py 02011D0173-20260331
	$(PYTHON) scripts/parse_32011R0753.py 02011R0753-20240913
	$(PYTHON) scripts/parse_32013R0401.py 02013R0401-20260429
	$(PYTHON) scripts/parse_32014R0224.py 02014R0224-20250326
	$(PYTHON) scripts/parse_32014R0833.py 02014R0833-20260717
	$(PYTHON) scripts/parse_32015R1755.py 02015R1755-20250924
	$(PYTHON) scripts/parse_32016R0044.py 02016R0044-20260616
	$(PYTHON) scripts/parse_32016R1686.py 02016R1686-20251124
	$(PYTHON) scripts/parse_32017R1770.py 02017R1770-20251203
	$(PYTHON) scripts/parse_32020R1998.py 02020R1998-20260528
	$(PYTHON) scripts/parse_32024R1485.py 02024R1485-20260713
	$(PYTHON) scripts/parse_32024R2642.py 02024R2642-20260713

validate:
	$(PYTHON) validate_csv.py

.PHONY: all parse validate
