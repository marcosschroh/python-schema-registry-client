#!/bin/sh

export PREFIX=""
if [ -d 'venv' ] ; then
    export PREFIX="venv/bin/"
fi

set -x

${PREFIX}flake8 .
${PREFIX}black --check schema_registry tests setup.py
${PREFIX}autoflake --in-place --recursive schema_registry tests setup.py
${PREFIX}isort --multi-line=3 --trailing-comma --force-grid-wrap=0 --combine-as --line-width 88 --recursive --apply schema_registry tests setup.py
${PREFIX}mypy schema_registry --ignore-missing-imports --disallow-untyped-defs
