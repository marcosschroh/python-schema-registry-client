#!/bin/sh

pytest --cov=schema_registry ${1} --cov-fail-under=90 && codecov && codecov --token=$CODECOV_TOKEN
