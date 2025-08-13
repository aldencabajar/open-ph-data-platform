from pathlib import Path

from airflow.sdk import Variable, dag, task

from opendata_ph.airflow_utils import create_bash_python_command, create_dag_id
from opendata_ph.constants import OpenDataPHConstants

build_folder = Variable.get(OpenDataPHConstants.BUILD_FOLDER_VAR)
ducklake_metadata_conn = Variable.get(OpenDataPHConstants.DUCKLAKE_METADATA_CONN)
duckdb_process_pool_name = Variable.get(OpenDataPHConstants.DUCKDB_PROCESS_POOL)

METADATA_JSON_PATH = f"{build_folder}/raw/psa/metadata.json"


@dag(dag_id=create_dag_id(__file__), schedule=None)
def psa_ingest():

    @task.bash(pool=duckdb_process_pool_name, cwd=str(Path.cwd().absolute()))
    def parse_geographical_codes():

        return create_bash_python_command(
            "ingest/psa_website/psa_geographical_codes.py",
            [build_folder, ducklake_metadata_conn],
        )

    @task.bash(pool=duckdb_process_pool_name, cwd=str(Path.cwd().absolute()))
    def parse_barangay_census_data():
        return create_bash_python_command(
            "ingest/psa_website/psa_barangay_census_data.py",
            [build_folder, METADATA_JSON_PATH, ducklake_metadata_conn],
        )

    parse_geographical_codes()
    parse_barangay_census_data()


psa_ingest()
