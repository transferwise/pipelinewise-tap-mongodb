.DEFAULT_GOAL := setup

create_venv:
	rm -Rf venv
	virtualenv -p python3 ./venv

setup_local_db:
	chmod u+x bin/setup_local_db.sh
	./bin/setup_local_db.sh

upgrade_pip:
	. venv/bin/activate; \
	python3 -m pip install -U pip setuptools

populate_db:
	venv/bin/python3 ./bin/populate_test_database.py etl secret@1

install_dep:
	. venv/bin/activate; \
	python3 -m pip install -e .[test,dev]

setup: create_venv setup_local_db upgrade_pip install_dep
	. venv/bin/activate; \
	python3 -m pip check && echo "No conflicts" || exit 1

pylint:
	pylint tap_mongodb tap_mongodb/sync_strategies --rcfile=pylintrc; \

test:
	pytest tests -v; \

test_cov:
	coverage run -m pytest tests -v; \
	coverage html -d coverage_html --include="tap_mongodb/*";

