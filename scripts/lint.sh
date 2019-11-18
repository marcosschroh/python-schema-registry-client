#!/bin/sh

flake8 .
black --check schema_registry tests setup.py
autoflake --in-place --recursive schema_registry tests setup.py
isort --multi-line=3 --trailing-comma --force-grid-wrap=0 --combine-as --line-width 88 --recursive --apply schema_registry tests setup.py
