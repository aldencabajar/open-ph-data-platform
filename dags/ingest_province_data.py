from datetime import datetime
from pathlib import Path
from uuid import uuid4

import duckdb
from airflow.sdk import Variable, dag, task

from opendata_ph.airflow_utils import create_bash_python_command, create_dag_id
from opendata_ph.constants import OpenDataPHConstants
from opendata_ph.duckdb import initialize_duckdb_catalog
from opendata_ph.wikipedia import get_last_edit_timestamp

build_folder = Variable.get(OpenDataPHConstants.BUILD_FOLDER_VAR)
ducklake_metadata_conn = Variable.get(OpenDataPHConstants.DUCKLAKE_METADATA_CONN)
duckdb_process_pool_name = Variable.get(OpenDataPHConstants.DUCKDB_PROCESS_POOL)

PARENT_PATH = f"landing/wikipedia/province_data/"
WIKIPEDIA_LINK = "https://en.wikipedia.org/wiki/Provinces_of_the_Philippines"

TABLE_NAME = "wikipedia_province_data"
SCHEMA = "bronze"


@dag(dag_id=create_dag_id(__file__), schedule=None)
def ingest_province_data():
    catalog = initialize_duckdb_catalog(ducklake_metadata_conn)

    path_to_write = (
        Path(build_folder)
        / PARENT_PATH
        / f"load_date={datetime.now().strftime('%Y-%m-%d')}"
        / f"{str(uuid4())}.csv"
    )

    check_if_need_to_ingest(catalog) >> scrape_wikipedia_province_data(str(path_to_write.absolute())) >> create_bronze_view(catalog)  # type: ignore


@task.short_circuit
def check_if_need_to_ingest(catalog_name: str):
    is_exists = duckdb.sql(
        f"""
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = '{TABLE_NAME}' and table_schema = '{SCHEMA}'
    ) as exists;
    """
    ).to_df()["exists"][0]

    if not is_exists:
        return True

    max_load_ts: datetime = duckdb.sql(
        f"""
        SELECT max(load_datetime_utc) as load_dt
        FROM {catalog_name}.{SCHEMA}.{TABLE_NAME}
        """
    ).to_df()["load_dt"][0]

    last_edit_timestamp = get_last_edit_timestamp(WIKIPEDIA_LINK)

    if max_load_ts < last_edit_timestamp:
        return True

    return False


@task.bash(pool=duckdb_process_pool_name, cwd=str(Path.cwd().absolute()))
def scrape_wikipedia_province_data(path_to_write: str):

    return create_bash_python_command(
        "ingest/wikipedia/province_data.py", [WIKIPEDIA_LINK, path_to_write]
    )


@task
def create_bronze_view(catalog: str):
    duckdb.sql(
        f"""
        CREATE OR REPLACE VIEW {catalog}.{SCHEMA}.{TABLE_NAME} AS (
            SELECT *
            FROM read_csv('{build_folder}/{PARENT_PATH}/*/*.csv', hive_partitioning=true)
        )
        """
    )


ingest_province_data()
