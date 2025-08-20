.PHONY: install run test lint clean docker-build docker-run

install:
	poetry install

run:
	poetry run spotify-to-tidal