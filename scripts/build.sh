#!/bin/bash

export AIRFLOW_VAR_BUILD_FOLDER="$(pwd)/_build"
export AIRFLOW_HOME="${AIRFLOW_VAR_BUILD_FOLDER}/airflow"
export AIRFLOW_VAR_DUCKLAKE_METADATA_CONN="sqlite:_build/metadata.sqlite"
export AIRFLOW_VAR_DUCKDB_PROCESS_POOL="duckdb_process"


airflow pools set "$AIRFLOW_VAR_DUCKDB_PROCESS_POOL" 1 "pool for running duckdb processes"
airflow standalone


