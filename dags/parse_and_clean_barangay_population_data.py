import logging
import os
from pathlib import Path
from typing import List

import duckdb
from airflow.decorators import setup, teardown
from airflow.sdk import Variable, dag, task

from opendata_ph.constants import OpenDataPHConstants
from opendata_ph.duckdb import initialize_duckdb_catalog

TEMP_SCHEMA = "temp_barangay_census_schema"
logger = logging.getLogger(__name__)

build_folder = Variable.get(OpenDataPHConstants.BUILD_FOLDER_VAR)
ducklake_metadata_conn = Variable.get(OpenDataPHConstants.DUCKLAKE_METADATA_CONN)
duckdb_process_pool_name = Variable.get(OpenDataPHConstants.DUCKDB_PROCESS_POOL)


@dag(dag_id=os.path.basename(__file__), schedule=None)
def ingest_and_clean_barangay_population_data():

    path_to_excel_files = Path(build_folder) / "raw/psa"

    catalog = initialize_duckdb_catalog(ducklake_metadata_conn)

    create_temp_schema_task = create_temp_schema(catalog)

    ingest_tasks = []
    table_names = []
    for path in path_to_excel_files.glob("*.xlsx"):

        region_name = get_region_name(path)
        table_name = f"{TEMP_SCHEMA}.{region_name.lower()}"
        table_names += [table_name]

        ingest_task = parse_barangay_population_data.override(
            task_id=f"parse_{region_name}"
        )(str(path.absolute()), table_name, ducklake_metadata_conn, region_name)

        ingest_tasks.append(ingest_task)

    drop_temp_schema_task = drop_temp_schema(catalog)
    union_all_temp_tables_task = union_all_temp_tables(table_names, catalog)

    create_temp_schema_task >> ingest_tasks >> union_all_temp_tables_task >> drop_temp_schema_task  # type: ignore


@setup
def create_temp_schema(catalog_name: str):
    duckdb.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{TEMP_SCHEMA};")


@teardown
def drop_temp_schema(catalog_name: str):
    duckdb.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{TEMP_SCHEMA} CASCADE;")


@task.bash(pool=duckdb_process_pool_name, cwd=str(Path.cwd().absolute()))
def parse_barangay_population_data(
    path_to_file: str, table_name: str, catalog_conn: str, region_label: str
) -> str:
    return (
        f"{Path.cwd()}/.venv/bin/python "
        f"{Path.cwd()}/ingest/psa_website/parse_barangay_census_data.py "
        f"'{path_to_file}' '{table_name}' '{catalog_conn}' '{region_label}'"
    )


@task
def union_all_temp_tables(table_names: List[str], catalog: str):
    sql = "\nUNION ALL\n".join(
        [f"SELECT * FROM {catalog}.{table_name}" for table_name in table_names]
    )
    wrapped = (
        f"""CREATE OR REPLACE TABLE {catalog}.bronze.barangay_census_data AS ({sql})"""
    )

    logger.info("sql to run: %s", wrapped)
    duckdb.sql(wrapped)


def get_region_name(file_name: Path) -> str:
    stem = file_name.stem
    return stem.split("_")[0].replace(" ", "_")


ingest_and_clean_barangay_population_data()
