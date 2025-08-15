#!/bin/bash

set -eo pipefail

init-db() {
    echo 'Initializing metadata db...'
    duckdb < ./scripts/startup.sql
}


init-db

BUILD_FOLDER="$(pwd)/_build"
DUCKLAKE_METADATA_CONN="sqlite:_build/metadata.sqlite"
PYTHON_VENV="./.venv/bin/python"

ingest-psa-website() {
    echo "Ingesting PSA Barangay Census Data..."
    ${PYTHON_VENV} ingest/psa_website/psa_barangay_census_data.py "${BUILD_FOLDER}" "${BUILD_FOLDER}/landing/psa/metadata.json" "${DUCKLAKE_METADATA_CONN}"

    printf '\nIngesting Philippine standard geographic codes...\n'
    ${PYTHON_VENV} ingest/psa_website/psa_geographical_codes.py "${BUILD_FOLDER}" "${BUILD_FOLDER}/landing/psa/metadata.json" "${DUCKLAKE_METADATA_CONN}"

}

ingest-wikipedia() {
    echo "Ingesting Wikipedia Province Data..."
    ${PYTHON_VENV} ingest/wikipedia/wikipedia_province_data.py "${BUILD_FOLDER}" "${DUCKLAKE_METADATA_CONN}"

    printf '\nIngesting Wikipedia City/Municipal Data\n'
    ${PYTHON_VENV} ingest/wikipedia/wikipedia_city_municipality.py "${BUILD_FOLDER}" "${DUCKLAKE_METADATA_CONN}"
}

ingest() {
    ingest-psa-website
    ingest-wikipedia
}

staging() {
    printf "\nRunning dbt staging models...\n"
    dbt run --select staging
    dbt test --select staging
}

final() {
    printf "\nRunning dbt final models...\n"
    dbt run --select final
    dbt test --select final
}

"$@"


