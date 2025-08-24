#!/bin/bash

set -eo pipefail

WORKING_DIR="$(pwd)"
BUILD_FOLDER="${WORKING_DIR}/_build"

DUCKLAKE_METADATA_CONN="sqlite:metadata.sqlite"
PYTHON_VENV="${WORKING_DIR}/.venv/bin/python"

init-db() {
    cd "$BUILD_FOLDER"
    echo 'Initializing metadata db...'
    duckdb < "${WORKING_DIR}/scripts/startup.sql"
}


init-db


ingest-psa-website() {
    cd "$BUILD_FOLDER"
    echo "Ingesting PSA Barangay Census Data..."
    ${PYTHON_VENV} "${WORKING_DIR}/ingest/psa_website/psa_barangay_census_data.py" "${BUILD_FOLDER}" "${BUILD_FOLDER}/landing/psa/metadata.json" "${DUCKLAKE_METADATA_CONN}"

    printf '\nIngesting Philippine standard geographic codes...\n'
    ${PYTHON_VENV} "${WORKING_DIR}/ingest/psa_website/psa_geographical_codes.py" "${BUILD_FOLDER}" "${BUILD_FOLDER}/landing/psa/metadata.json" "${DUCKLAKE_METADATA_CONN}"

}

ingest-wikipedia() {
    cd "$BUILD_FOLDER"
    echo "Ingesting Wikipedia Province Data..."
    ${PYTHON_VENV} "${WORKING_DIR}/ingest/wikipedia/wikipedia_province_data.py" "${BUILD_FOLDER}" "${DUCKLAKE_METADATA_CONN}"

    printf '\nIngesting Wikipedia City/Municipal Data\n'
    ${PYTHON_VENV} "${WORKING_DIR}/ingest/wikipedia/wikipedia_city_municipality.py" "${BUILD_FOLDER}" "${DUCKLAKE_METADATA_CONN}"
}

ingest() {
    ingest-psa-website
    ingest-wikipedia
}

staging() {
    cd "$BUILD_FOLDER"
    printf "\nRunning dbt staging models...\n"
    dbt run --select staging --project-dir "$WORKING_DIR/transform"
    dbt test --select staging --project-dir "$WORKING_DIR/transform"
}

final() {
    cd "$BUILD_FOLDER"
    printf "\nRunning dbt final models...\n"
    dbt run --select final --project-dir "$WORKING_DIR/transform"
    dbt test --select final --project-dir "$WORKING_DIR/transform"
}


build() {
    ingest
    staging
    final

}

"$@"



