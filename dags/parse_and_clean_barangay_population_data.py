import logging
import os
from pathlib import Path

import duckdb
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



    create_temp_schema_task = create_temp_schema(ducklake_metadata_conn)

    ingest_tasks = []
    for path in path_to_excel_files.glob("*.xlsx"):

        region_name = get_region_name(path)
        table_name = f"{TEMP_SCHEMA}.{region_name.lower()}"
        ingest_task = parse_barangay_population_data.override(task_id=f"parse_{region_name}")(
            str(path.absolute()), table_name, ducklake_metadata_conn
        )
        ingest_tasks.append(ingest_task)
    
    create_temp_schema_task >> ingest_tasks #type: ignore
    
@task
def create_temp_schema(ducklake_metadata_conn: str):
    catalog = initialize_duckdb_catalog(ducklake_metadata_conn)
    logger.info("current execution path %s", Path.cwd())
    duckdb.sql(f"DROP SCHEMA IF EXISTS {catalog}.{TEMP_SCHEMA} CASCADE;")
    duckdb.sql(f"CREATE SCHEMA {catalog}.{TEMP_SCHEMA};")

@task.bash(pool=duckdb_process_pool_name, cwd=str(Path.cwd().absolute()))
def parse_barangay_population_data(
    path_to_file: str, table_name: str, catalog_conn: str
) -> str:
    return f"{Path.cwd()}/.venv/bin/python {Path.cwd()}/ingest/psa_website/parse_barangay_census_data.py '{path_to_file}' '{table_name}' '{catalog_conn}'"


def get_region_name(file_name: Path) -> str:
    stem = file_name.stem
    return stem.split("_")[0].replace(" ", "_")


ingest_and_clean_barangay_population_data()
