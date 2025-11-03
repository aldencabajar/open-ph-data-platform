import sys
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import Callable, Protocol

import duckdb
import openpyxl
import pandas as pd
import pytz
from openpyxl.workbook.workbook import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell.rich_text import CellRichText
from openpyxl.cell.cell import Cell

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

        wb = openpyxl.load_workbook(path_to_ws, rich_text=True)
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
            SELECT * FROM read_csv('{PATH_TO_WRITE}', hive_partitioning=true)
        
        )
        """
    )
    logger.info("table: '%s' created", full_table_name)


def get_region_name(file_name: Path) -> str:
    stem = file_name.stem
    return stem.split("_")[0].replace(" ", "_")


def _extract_value_from_rich_text(cell_value: CellRichText) -> str:
    value = ""
    for text_run in cell_value:
        if isinstance(text_run, str):
            value += text_run
    return value


def _extract_value_from_str(cell_value: str) -> str:
    """We sanitize strings further to remove unicode characters.

    Args:
        cell_value (str): The cell value

    Returns:
        str: A sanitized string
    """
    SUPERSCRIPT_CHARS = "⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾ᵃᵇᶜᵈᵉᶠᵍʰⁱʲᵏˡᵐⁿᵒᵖ𐞥ʳˢᵗᵘᵛʷˣʸᶻᴬᴮᴰᴱᴳᴴᴵᴶᴷᴸᴹᴺᴼᴾᴿᵀᵁⱽᵂ"
    sanitized = ""
    for char in cell_value:
        if char not in SUPERSCRIPT_CHARS:
            sanitized += char
    return sanitized


def process_sheet(
    province_identifier: Callable,
    city_municipality_identifier: Callable,
    barangay_identifier: Callable,
    sheet: Worksheet,
) -> pd.DataFrame:
    rows = list(sheet.rows)

    province = None
    city_municipality = None
    data = []
    for i in range(len(rows)):
        row_data = {}
        for j, cell in enumerate(rows[i]):
            if isinstance(cell.value, str) or isinstance(cell.value, list):
                value = (
                    _extract_value_from_rich_text(cell.value)
                    if isinstance(cell.value, list)
                    else _extract_value_from_str(cell.value)
                )
                if province_identifier(value, rows, i, j):
                    province = value
                elif city_municipality_identifier(value, rows, i, j):
                    city_municipality = value
                elif barangay_identifier(value, rows, i, j):
                    row_data["barangay"] = value

            if isinstance(cell.value, int):
                row_data["population"] = cell.value
        if city_municipality:
            row_data["city_municipality"] = city_municipality
        row_data["province"] = province

        PRESENT_KEYS = ["barangay", "population", "city_municipality"]

        if all(key in row_data for key in PRESENT_KEYS):
            data.append(row_data)

    return pd.DataFrame(data)


# default identifiers
default_province_identifier = lambda value, rows, i, j: (
    value.isupper() and rows[i - 1][j].value is None and rows[i + 1][j].value is None
)
default_city_municipality_identifier = (
    lambda value, rows, i, j: value.isupper()
    and rows[i - 1][j].value is None
    and rows[i + 1][j].value is not None
)
default_barangay_identifier = lambda value, rows, i, j: rows[i - 1][j].value is not None

# City of Manila specific identifiers
null_identifier = lambda cell, rows, i, j: False
manila_city_mun_identifier = (
    lambda value, rows, i, j: value.isupper()
    and rows[i - 1][j].value is None
    and rows[i + 1][j].value is None
)


def process_workbook(
    wb: Workbook, meta: ObjectMetadata, region_name: str, logger: Logger
) -> pd.DataFrame:

    dfs = []
    for sheet_name in wb.sheetnames:
        sheet = wb[sheet_name]
        if sheet.sheet_state != "visible":
            continue

        logger.info(f"processing sheet {sheet_name}...")

        if sheet_name == "City of Manila":
            df = process_sheet(
                null_identifier,
                manila_city_mun_identifier,
                default_barangay_identifier,
                wb[sheet_name],
            )
        else:
            df = process_sheet(
                default_province_identifier,
                default_city_municipality_identifier,
                default_barangay_identifier,
                wb[sheet_name],
            )
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
