#!/bin/sh

pytest --cov=schema_registry ${1} --cov-fail-under=85
codecov && codecov --token=$CODECOV_TOKEN
