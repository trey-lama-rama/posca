.PHONY: setup sync query report clean config-check

setup:
	bash setup.sh

sync:
	bash sync.sh

query:
	python3 tools/query.py $(ARGS)

report:
	python3 tools/crm_report.py

clean:
	rm -rf data/crm.db logs/*.log

config-check:
	python3 config.py
