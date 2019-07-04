#!/bin/sh

tests=${1-"./tests"}

pytest --cov=schema_registry ${tests}
codecov && codecov --token=$CODECOV_TOKEN