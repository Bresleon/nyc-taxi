MES ?= 2025-03
PYTHON ?= python3

.PHONY: all setup test ui clean

setup:
	$(PYTHON) -m pip install -r requirements.txt
