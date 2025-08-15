import asyncio
import re
import sys
from datetime import datetime, timezone
from logging import Logger
from pathlib import Path

import duckdb
import pandas as pd
from playwright.async_api import async_playwright

from opendata_ph.constants import DataLakeLayers
from opendata_ph.duckdb import initialize_duckdb_catalog
from opendata_ph.logger import create_logger
from opendata_ph.wikipedia import get_last_edit_timestamp, scrape_wikipedia_table

WIKIPEDIA_LINK = (
    "https://en.wikipedia.org/wiki/List_of_cities_and_municipalities_in_the_Philippines"
)
TABLE_NAME = "wikipedia_city_municipality"
PATH_TO_WRITE = "raw/wikipedia/wikipedia_city_municipality.csv"


async def main():
    build_folder = Path(sys.argv[1])
    ducklake_catalog_conn = sys.argv[2]

    catalog = initialize_duckdb_catalog(ducklake_catalog_conn)

    logger = create_logger("wikipedia_city_municipality")

    df = await scrape_wikipedia_city_municipality(WIKIPEDIA_LINK, logger)
    df["source_uri"] = WIKIPEDIA_LINK
    df["source_timestamp_utc"] = await get_last_edit_timestamp(WIKIPEDIA_LINK)
    df["load_datetime_utc"] = datetime.now(tz=timezone.utc)

    logger.info("writing %s rows", df.shape[0])

    full_path = build_folder / Path(PATH_TO_WRITE)
    full_path.parent.mkdir(exist_ok=True, parents=True)
    df.to_csv(full_path, index=False)

    logger.info("written to path %s", str(full_path.absolute()))

    full_table_name = f"{catalog}.{DataLakeLayers.RAW}.{TABLE_NAME}"

    logger.info("writing to table %s", full_table_name)

    duckdb.sql(
        f"""
        CREATE OR REPLACE VIEW {full_table_name} AS (
        SELECT * FROM read_csv('{build_folder}/{PATH_TO_WRITE}', hive_partitioning=true)
        )
        """
    )


def _clean_header(header: str) -> str:
    return (
        re.sub(r"\[.*\]|[^a-zA-Z-0-9-\s]", "", header).replace(" ", "_").strip().lower()
    )


async def scrape_wikipedia_city_municipality(link: str, logger: Logger) -> pd.DataFrame:

    async with async_playwright() as p:
        # Create a new browser context
        browser = await p.chromium.launch()
        page = await browser.new_page()

        # Navigate to the Wikipedia page
        await page.goto(link)

        logger.info("scraping %s", link)

        table = page.locator("table.wikitable").filter(has_text="City or municipality")

        df = await scrape_wikipedia_table(table, header_cleaner_func=_clean_header)

    df.dropna(inplace=True, how="any")

    return df


asyncio.run(main())
