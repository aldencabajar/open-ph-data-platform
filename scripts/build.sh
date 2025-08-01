#!/bin/bash

AIRFLOW_VAR_BUILD_FOLDER="$(pwd)/_build"
export AIRFLOW_HOME="${AIRFLOW_VAR_BUILD_FOLDER}/airflow"
export AIRFLOW_VAR_DUCKLAKE_SQLITE_PATH="${AIRFLOW_VAR_BUILD_FOLDER}/metadata.sqlite"


airflow pools set duckdb_process 1 "pool for running duckdb processes"
airflow standalone


