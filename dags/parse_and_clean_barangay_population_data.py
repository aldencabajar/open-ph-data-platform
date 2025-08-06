import os
import logging
import duckdb
from pathlib import Path

from airflow.sdk import Variable, dag, task

from opendata_ph.constants import OpenDataPHConstants
from opendata_ph.duckdb import initialize_duckdb_catalog

TEMP_SCHEMA = "temp_barangay_census_schema"
logger = logging.getLogger(__name__)


@dag(dag_id=os.path.basename(__file__), schedule=None)
def ingest_and_clean_barangay_population_data():

    build_folder = Variable.get(OpenDataPHConstants.BUILD_FOLDER_VAR)
    ducklake_catalog_conn = Variable.get(OpenDataPHConstants.DUCKLAKE_SQLITE_PATH)

    path_to_excel_files = Path(build_folder) / "raw/psa"

    catalog = initialize_duckdb_catalog(ducklake_catalog_conn)

    duckdb.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{TEMP_SCHEMA};")

    for path in path_to_excel_files.glob("*.xlsx"):

        region_name = get_region_name(path)
        table_name = f"{TEMP_SCHEMA}.{region_name.lower()}"
        parse_barangay_population_data.override(task_id=f"parse_{region_name}")(
            str(path.absolute()), table_name, ducklake_catalog_conn
        )
    
@task.bash(pool="duckdb_process")
def parse_barangay_population_data(
    path_to_file: str, table_name: str, catalog_conn: str
) -> str:
    return f"{Path.cwd()}/.venv/bin/python {Path.cwd()}/ingest/psa_website/parse_barangay_census_data.py '{path_to_file}' '{table_name}' '{catalog_conn}'"


def get_region_name(file_name: Path) -> str:
    stem = file_name.stem
    return stem.split("_")[0].replace(" ", "_")


ingest_and_clean_barangay_population_data()
