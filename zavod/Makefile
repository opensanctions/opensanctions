
test:
	pytest --cov-report html --cov-report term --cov=zavod zavod/tests/

typecheck:
	mypy --strict --exclude zavod/tests zavod/

lint:
	ruff check zavod/

check: test typecheck lint

clean:
	rm -rf .coverage htmlcov dist build zavod.egg-info