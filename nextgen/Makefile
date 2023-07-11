
test:
	pytest --cov-report html --cov-report term --cov=zavod tests/

typecheck:
	mypy --strict zavod/

check: test typecheck

clean:
	rm -rf .coverage htmlcov dist build zavod.egg-info