# Makefile pour le projet sqlite3-to-oracle

.PHONY: install dev-install clean test build publish help lint

# Variables
PYTHON = python3
PIP = $(PYTHON) -m pip
PYTEST = $(PYTHON) -m pytest
BUILD_DIR = dist
PACKAGE_NAME = sqlite3-to-oracle

help:
	@echo "Commandes disponibles:"
	@echo "  make install       : Installation de l'outil"
	@echo "  make dev-install   : Installation en mode développeur"
	@echo "  make test          : Lancement des tests"
	@echo "  make clean         : Suppression des fichiers de compilation et de build"
	@echo "  make build         : Construction du package"
	@echo "  make publish       : Publication du package sur PyPI"
	@echo "  make lint          : Vérification du code avec flake8"

install:
	$(PIP) install .

dev-install:
	$(PIP) install -e ".[dev,ui]"

clean:
	rm -rf $(BUILD_DIR)
	rm -rf *.egg-info
	rm -rf __pycache__
	rm -rf .pytest_cache
	find . -name "__pycache__" -type d -exec rm -rf {} +
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete
	find . -name "*.pyd" -delete

test:
	$(PYTEST) tests/

build: clean
	$(PYTHON) setup.py sdist bdist_wheel

publish: build
	$(PIP) install twine
	twine check $(BUILD_DIR)/*
	twine upload $(BUILD_DIR)/*

lint:
	$(PYTHON) -m flake8 $(PACKAGE_NAME)
