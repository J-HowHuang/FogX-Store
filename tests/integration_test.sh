#!/bin/bash

# This script is used to run integration tests for the application.
curl ${SKULK_URL}:11632

python prepare_data.py http://${COLLECTOR_URL}:11635

curl ${SKULK_URL}:11632/datasets

python example_client.py ${SKULK_URL}:50052