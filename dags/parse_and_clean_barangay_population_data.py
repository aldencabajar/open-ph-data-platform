import os
import logging
from pathlib import Path

from airflow.sdk import Variable, dag, task

from opendata_ph.constants import OpenDataPHConstants

logger = logging.getLogger(__name__)

@dag(dag_id=os.path.basename(__file__), schedule=None)
def ingest_and_clean_barangay_population_data():

    build_folder = Variable.get(OpenDataPHConstants.BUILD_FOLDER_VAR)

    path_to_excel_files = Path(build_folder) / "raw/psa"
    write_base_path = Path(build_folder) / "bronze/psa/barangay-census-data"

    write_base_path.mkdir(parents=True, exist_ok=True)

    for path in path_to_excel_files.glob("*.xlsx"):

        region_name = get_region_name(path)
        path_to_write = write_base_path / (region_name + ".snappy.parquet")
        parse_barangay_population_data.override(task_id=f"parse_{region_name}")(
            str(path.absolute()), str(path_to_write.absolute())
        )


@task.bash
def parse_barangay_population_data(path_to_file: str, output_path: str) -> str:
    return f"{Path.cwd()}/.venv/bin/python {Path.cwd()}/ingestion/psa_website/parse_barangay_census_data.py '{path_to_file}' '{str(output_path)}'"


def get_region_name(file_name: Path) -> str:
    stem = file_name.stem
    return stem.split("_")[0].replace(" ", "_")


ingest_and_clean_barangay_population_data()
