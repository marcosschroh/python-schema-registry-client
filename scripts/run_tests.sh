#!/bin/sh

pytest --cov=schema_registry ${1}
codecov && codecov --token=$CODECOV_TOKEN