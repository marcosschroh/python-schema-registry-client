#!/bin/sh

set -o errexit

docker-compose build

tests=${1-"./tests"}

# run tests against schema registry server
docker-compose run -e CODECOV_TOKEN=${CODECOV_TOKEN} schema-registry-client \
    pytest --cov=schema_registry ${tests}; \
    codecov && codecov --token=$CODECOV_TOKEN