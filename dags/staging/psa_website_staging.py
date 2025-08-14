from pathlib import Path
from airflow.sdk import Variable, dag, task

from opendata_ph.airflow_utils import create_dag_id
from opendata_ph.constants import OpenDataPHConstants

ducklake_metadata_conn = Variable.get(OpenDataPHConstants.DUCKLAKE_METADATA_CONN)
duckdb_process_pool_name = Variable.get(OpenDataPHConstants.DUCKDB_PROCESS_POOL)


@dag(dag_id=create_dag_id(__file__), schedule=None)
def psa_website_staging():

    @task.bash(pool=duckdb_process_pool_name, cwd=str(Path.cwd().absolute()))
    def build_models():
        return f"dbt run --select staging.psa_website"
    
    @task.bash(pool=duckdb_process_pool_name, cwd=str(Path.cwd().absolute()))
    def test_models():
        return f"dbt test --select staging.psa_website"
    
    build_models() >> test_models() #type: ignore

psa_website_staging()
        
