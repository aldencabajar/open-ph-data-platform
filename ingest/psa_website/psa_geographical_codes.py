from importlib import metadata
import logging
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd
import pytz

from opendata_ph.constants import DataLakeLayers
from opendata_ph.duckdb import initialize_duckdb_catalog
from opendata_ph.logger import create_logger
from opendata_ph.metadata import parse_metadata

RELEVANT_SHEET_NAME = "PSGC"
FILE_TO_PARSE = "landing/psa/PSGC-2Q-2025-Publication-Datafile.xlsx"
PATH_TO_WRITE = "raw/psa/psa_geographical_codes.csv"
TABLE_NAME = "psa_geographical_codes"


def main():
    build_folder = Path(sys.argv[1])
    metadata_file_path = Path(sys.argv[2])
    ducklake_catalog_conn = sys.argv[3]

    catalog = initialize_duckdb_catalog(ducklake_catalog_conn)
    logger = create_logger("psa_geographical_codes")

    # get metadata object to enrich the dataset
    meta = parse_metadata(Path(metadata_file_path), build_folder / Path(FILE_TO_PARSE))

    logger.info("parsing file %s", FILE_TO_PARSE)

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

    csv_write_path = build_folder / PATH_TO_WRITE

    logger.info("writing to %s", csv_write_path)

    csv_write_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(csv_write_path, index=False)

    full_table_name = f"{catalog}.{DataLakeLayers.RAW}.{TABLE_NAME}"
    duckdb.sql(
        f"""
        CREATE OR REPLACE VIEW {full_table_name}  AS (
            SELECT * FROM read_csv('{PATH_TO_WRITE}', hive_partitioning=true)
        
        )
        """
    )
    logger.info("table: '%s' created", full_table_name)


if __name__ == "__main__":
    main()
