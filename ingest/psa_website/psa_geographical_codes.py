import logging
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd
import pytz

from opendata_ph.airflow_utils import check_if_table_exists, check_if_table_is_stale
from opendata_ph.constants import DataLakeLayers
from opendata_ph.duckdb import initialize_duckdb_catalog
from opendata_ph.logger import create_logger
from opendata_ph.metadata import parse_metadata

RELEVANT_SHEET_NAME = "PSGC"
FILE_TO_PARSE = "raw/psa/PSGC-2Q-2025-Publication-Datafile.xlsx"
METADATA_PATH = "raw/psa/metadata.json"
PARENT_PATH_TO_WRITE = "landing/psa/geographical_codes/"
TABLE_NAME = "psa_geographical_codes"


def main():
    build_folder = Path(sys.argv[1])
    ducklake_catalog_conn = sys.argv[2]

    catalog = initialize_duckdb_catalog(ducklake_catalog_conn)
    logger = create_logger("psa_geographical_codes")

    # get metadata object to enrich the dataset
    meta = parse_metadata(
        build_folder / Path(METADATA_PATH), build_folder / Path(FILE_TO_PARSE)
    )

    table_exists = check_if_table_exists(DataLakeLayers.BRONZE, TABLE_NAME)

    if table_exists:
        is_table_stale = check_if_table_is_stale(
            catalog,
            TABLE_NAME,
            DataLakeLayers.BRONZE,
            "load_datetime_utc",
            meta.source_timestamp,
        )
        print(meta.source_timestamp)
        if not is_table_stale:
            logger.info("exiting early since table is not stale.")
            return

    df = pd.read_excel(build_folder / FILE_TO_PARSE, sheet_name=RELEVANT_SHEET_NAME)
    # remove the uninformative column, two indices from the right
    df = df.drop(df.columns[-2], axis=1)

    df.columns = [
        "psgc",
        "name",
        "correspondence_code",
        "geographic_level",
        "old_names",
        "city_class",
        "income_classification",
        "urban_rural_class",
        "population",
        "status",
    ]

    df["retrieved_timestamp_utc"] = meta.retrieved_timestamp.astimezone(pytz.utc)
    df["source_uri"] = meta.source_uri
    df["source_timestamp_utc"] = meta.source_timestamp.astimezone(pytz.utc)
    df["load_datetime_utc"] = datetime.now()

    csv_write_path = (
        build_folder
        / PARENT_PATH_TO_WRITE
        / f"load_date={datetime.now().strftime("%Y-%m-%d")}"
        / f"{str(uuid4())}.csv"
    )

    csv_write_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(csv_write_path, index=False)

    full_table_name = f"{catalog}.{DataLakeLayers.BRONZE}.{TABLE_NAME}"
    duckdb.sql(
        f"""
        CREATE OR REPLACE VIEW {full_table_name}  AS (
            SELECT * FROM read_csv('{build_folder}/{PARENT_PATH_TO_WRITE}/*/*.csv', hive_partitioning=true)
        
        )
        """
    )
    logger.info("table: '%s' created", full_table_name)


if __name__ == "__main__":
    main()
