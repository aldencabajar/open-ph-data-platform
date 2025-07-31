import sys
from logging import Logger
from pathlib import Path

import openpyxl
import pandas as pd
from openpyxl.workbook.workbook import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from opendata_ph.logger import create_logger

PATH_TO_WRITE = "bronze/psa-website/"


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


def process_workbook(wb: Workbook, logger: Logger):

    dfs = []
    for sheet_name in wb.sheetnames:
        sheet = wb[sheet_name]
        if sheet.sheet_state == "visible":
            logger.info(f"processing sheet {sheet_name}...")
            df = process_sheet(wb[sheet_name])
        dfs.append(df)
    
    return pd.concat(dfs)


def main():
    path_to_census_data = Path(sys.argv[1])
    path_to_write = Path(sys.argv[2])
    logger = create_logger("census_data_extraction")

    wb = openpyxl.load_workbook(path_to_census_data)

    result = process_workbook(wb, logger)

    path_to_write.parent.mkdir(exist_ok=True, parents=True)
    result.to_parquet(path_to_write, compression="snappy", index=False)

    logger.info("written to path %s", str(path_to_write.absolute()))


if __name__ == "__main__":
    main()
