from pathlib import Path

from airflow.sdk import Variable, dag, task

from opendata_ph.airflow_utils import create_bash_python_command, create_dag_id
from opendata_ph.constants import OpenDataPHConstants

build_folder = Variable.get(OpenDataPHConstants.BUILD_FOLDER_VAR)
ducklake_metadata_conn = Variable.get(OpenDataPHConstants.DUCKLAKE_METADATA_CONN)
duckdb_process_pool_name = Variable.get(OpenDataPHConstants.DUCKDB_PROCESS_POOL)


@dag(dag_id=create_dag_id(__file__), schedule=None)
def psa_ingest():

    parse_geographical_codes()


@task.bash(pool=duckdb_process_pool_name, cwd=str(Path.cwd().absolute()))
def parse_geographical_codes():

    return create_bash_python_command(
        "ingest/psa_website/psa_geographical_codes.py",
        [build_folder, ducklake_metadata_conn],
    )


psa_ingest()
