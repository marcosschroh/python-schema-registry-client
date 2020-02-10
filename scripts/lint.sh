#!/bin/sh

export PREFIX=""
if [ -d 'venv' ] ; then
    export PREFIX="venv/bin/"
fi

set -x

${PREFIX}flake8 .
${PREFIX}black --line-length 120 --check .
${PREFIX}autoflake --in-place --recursive schema_registry tests setup.py
${PREFIX}isort -rc --line-width 120 .
${PREFIX}mypy schema_registry --ignore-missing-imports --disallow-untyped-defs
