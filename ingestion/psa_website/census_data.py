from pathlib import Path
import os
import openpyxl
import pandas as pd
from opendata_ph.logger import create_logger

PATH_TO_CENSUS_DATA = "raw/population-by-province-city-and-municipality.xlsx"


def main():
    logger = create_logger("census_data_extraction")

    build_folder_path = Path(os.environ["BUILD_FOLDER"])

    census_data_path = build_folder_path / PATH_TO_CENSUS_DATA
    wb = openpyxl.load_workbook(census_data_path)

    dfs = []

    for sheet_name in wb.sheetnames:
        logger.info(f"processing sheet {sheet_name}...")
        df = pd.read_excel(census_data_path, sheet_name=sheet_name)
        processed = (
            df.iloc[:, :6].drop(df.columns[1], axis=1).dropna().reset_index(drop=True)
        )
        processed.columns = [
            "administrative_unit",
            "population_2010",
            "population_2015",
            "population_2020",
            "population_2024",
        ]
        processed["region"] = sheet_name
        dfs.append(processed)

    merged = pd.concat(dfs)

    (build_folder_path / "bronze" / "psa-website").mkdir(exist_ok=True, parents=True)

    merged.to_csv(
        build_folder_path / "bronze/psa-website/census-data.csv.gz", compression="gzip"
    )


if __name__ == "__main__":
    main()
