#!/bin/bash

ingest-psa-website() {
    cd "$BUILD_FOLDER"
    echo "Ingesting PSA Barangay Census Data..."
    ${VENV_PYTHON} "${WORKING_DIR}/ingest/psa_website/psa_barangay_census_data.py" "${BUILD_FOLDER}" "${BUILD_FOLDER}/landing/psa/metadata.json" "${DL_METADATA_CONN}"

    printf '\nIngesting Philippine standard geographic codes...\n'
    ${VENV_PYTHON} "${WORKING_DIR}/ingest/psa_website/psa_geographical_codes.py" "${BUILD_FOLDER}" "${BUILD_FOLDER}/landing/psa/metadata.json" "${DL_METADATA_CONN}"

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
    $DBT_PATH run --select staging --project-dir "$WORKING_DIR/transform"
}

final() {
    cd "$BUILD_FOLDER"
    printf "\nRunning dbt final models...\n"
    $DBT_PATH run --select final --project-dir "$WORKING_DIR/transform"
}


test() {
    cd "$BUILD_FOLDER"
    printf "\nRunning dbt tests...\n"
    if [ -z "$1" ]; then
        $DBT_PATH test --project-dir "$WORKING_DIR/transform"
        return
    fi
    $DBT_PATH test --project-dir "$WORKING_DIR/transform" --select "$1"
}



