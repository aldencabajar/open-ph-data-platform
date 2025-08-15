import asyncio
import re
import sys
from datetime import datetime
from logging import Logger
from pathlib import Path

import duckdb
import pandas as pd
from playwright.async_api import async_playwright

from opendata_ph.constants import DataLakeLayers
from opendata_ph.duckdb import (
    check_if_table_exists,
    check_if_table_is_stale,
    initialize_duckdb_catalog,
)
from opendata_ph.logger import create_logger
from opendata_ph.wikipedia import get_last_edit_timestamp, merge_multiple_header_rows

PATH_TO_WRITE = "raw/wikipedia/wikipedia_province_data.csv"
TABLE_NAME = "wikipedia_province_data"
WIKIPEDIA_LINK = "https://en.wikipedia.org/wiki/Provinces_of_the_Philippines"

async def main():
    build_folder = Path(sys.argv[1])
    ducklake_catalog_conn = sys.argv[2]

    catalog = initialize_duckdb_catalog(ducklake_catalog_conn)

    logger = create_logger("wikipedia_province_data")

    logger.info("scraping %s", WIKIPEDIA_LINK)

    df = await scrape_wikipedia_province_data(WIKIPEDIA_LINK, logger)

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
    # split by new line
    splitted = header.split("\n")
    new_strs = []
    for _str in splitted:
        new_strs += [re.sub(r"\[.*\]", "", _str)]

    return " ".join(new_strs)


async def check_if_need_to_scrape(
    wiki_link: str, catalog_name: str, table_name: str, schema: str
) -> bool:
    is_exists = check_if_table_exists(schema, table_name)

    if not is_exists:
        return True

    last_edit_timestamp = await get_last_edit_timestamp(wiki_link)

    is_stale = check_if_table_is_stale(
        catalog=catalog_name,
        table=table_name,
        schema=schema,
        timestamp_column="load_datetime_utc",
        timestamp_to_compare=last_edit_timestamp,
    )

    if is_stale:
        return True

    return False


async def scrape_wikipedia_province_data(
    wiki_link: str, logger: Logger
) -> pd.DataFrame:
    async with async_playwright() as p:

        browser = await p.chromium.launch()

        page = await browser.new_page()

        await page.goto(wiki_link)

        table = page.locator("table").filter(has_text="Province")
        header_texts = await table.locator("thead tr").all_inner_texts()

        headers = merge_multiple_header_rows(header_texts)

        rows = table.locator("tbody tr")

        table_data = await rows.evaluate_all(
            """(rowElements) => {
        return rowElements.map(row => {
            return Array.from(row.querySelectorAll('th, td')).map(cell => cell.textContent?.trim());
        });
    }"""
        )

        # process headers
        headers = list(filter(lambda header: header != "", map(_clean_header, headers)))

        logger.info("headers: %s", headers)

        # process rows
        table_data = filter(lambda row: len(row) == len(headers), table_data)

        df = pd.DataFrame(data=table_data, columns=headers)
        df["source"] = wiki_link
        df["load_datetime_utc"] = datetime.now()

        return df


asyncio.run(main())
