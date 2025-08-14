import json
import sys
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import Dict
from uuid import uuid4

import duckdb
import openpyxl
import pandas as pd
import pytz
from openpyxl.workbook.workbook import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from opendata_ph.constants import DataLakeLayers
from opendata_ph.duckdb import initialize_duckdb_catalog
from opendata_ph.logger import create_logger
from opendata_ph.metadata import (
    ObjectMetadata,
    get_object_metadata_by_key_contains,
    load_metadata,
)

TABLE_NAME = "psa_barangay_census_data"
PATH_TO_WRITE = "raw/psa/psa_barangay_census_data.csv"


def main():
    build_folder = Path(sys.argv[1])
    metadata_file_path = Path(sys.argv[2])
    ducklake_catalog_conn = sys.argv[3]

    logger = create_logger("psa_barangay_census_data")

    catalog = initialize_duckdb_catalog(ducklake_catalog_conn)

    # worksheets to process
    raw_files_metadata = load_metadata(metadata_file_path)

    ws_object_metadata = get_object_metadata_by_key_contains(
        raw_files_metadata, "2024_census_data"
    )

    dfs = []
    for rel_path, meta in ws_object_metadata.items():
        path_to_ws = metadata_file_path.parent / rel_path
        logger.info("reading workbook %s", path_to_ws)

        wb = openpyxl.load_workbook(path_to_ws)
        result = process_workbook(wb, meta, get_region_name(path_to_ws), logger)

        dfs.append(result)

    concatenated = pd.concat(dfs)
    concatenated["census_year"] = 2024

    # writing to csv
    csv_write_path = build_folder / PATH_TO_WRITE

    csv_write_path.parent.mkdir(parents=True, exist_ok=True)
    concatenated.to_csv(csv_write_path, index=False)

    # create or replace the view over the landed csv files
    full_table_name = f"{catalog}.{DataLakeLayers.RAW}.{TABLE_NAME}"
    duckdb.sql(
        f"""
        CREATE OR REPLACE VIEW {full_table_name}  AS (
            SELECT * FROM read_csv('{build_folder}/{PATH_TO_WRITE}', hive_partitioning=true)
        
        )
        """
    )
    logger.info("table: '%s' created", full_table_name)


def get_region_name(file_name: Path) -> str:
    stem = file_name.stem
    return stem.split("_")[0].replace(" ", "_")


def process_sheet(sheet: Worksheet) -> pd.DataFrame:
    rows = list(sheet.rows)

    province = None
    city_municipality = None
    data = []
    for i in range(len(rows)):
        row_data = {}
        for j, cell in enumerate(rows[i]):
            if isinstance(cell.value, str):
                if (
                    cell.value.isupper()
                    and rows[i - 1][j].value is None
                    and rows[i + 1][j].value is None
                ):
                    province = cell.value

                if cell.value.isupper() and rows[i + 1][j].value is not None:
                    city_municipality = cell.value

                if not cell.value.isupper():
                    row_data["barangay"] = cell.value

            if isinstance(cell.value, int):
                row_data["population"] = cell.value
        if not row_data:
            city_municipality = None
        if city_municipality:
            row_data["city_municipality"] = city_municipality
        row_data["province"] = province

        data.append(row_data)

    filtered = filter(
        lambda x: all(k in x for k in ["barangay", "population", "city_municipality"]),
        data,
    )

    return pd.DataFrame(filtered)


def process_workbook(
    wb: Workbook, meta: ObjectMetadata, region_name: str, logger: Logger
) -> pd.DataFrame:

    dfs = []
    for sheet_name in wb.sheetnames:
        sheet = wb[sheet_name]
        if sheet.sheet_state == "visible":
            logger.info(f"processing sheet {sheet_name}...")
            df = process_sheet(wb[sheet_name])
            dfs.append(df)
    result = pd.concat(dfs)
    result["region"] = region_name
    result["retrieved_timestamp_utc"] = meta.retrieved_timestamp.astimezone(pytz.utc)
    result["source_uri"] = meta.source_uri
    result["source_timestamp_utc"] = meta.source_timestamp.astimezone(pytz.utc)
    result["load_datetime_utc"] = datetime.now()

    return result


if __name__ == "__main__":
    main()
