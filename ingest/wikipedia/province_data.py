import asyncio
import re
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
from playwright.async_api import async_playwright

from opendata_ph.logger import create_logger
from opendata_ph.wikipedia import merge_multiple_header_rows

async def main():
    wiki_link = sys.argv[1]
    path_to_write = sys.argv[2]
    logger = create_logger("wikipedia_province_data_ingestion")

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
        
        # writing to csv
        df = pd.DataFrame(data=table_data, columns=headers)
        df["source"] = wiki_link
        df["load_datetime_utc"] = datetime.now()
        logger.info("writing %s rows", df.shape[0])
        full_path = Path(path_to_write)

        full_path.parent.mkdir(exist_ok=True, parents=True)

        df.to_csv(full_path, index=False)

        logger.info("written to path %s", str(full_path.absolute()))


def _clean_header(header: str) -> str:
    # split by new line
    splitted = header.split("\n")
    new_strs = []
    for _str in splitted:
        new_strs += [re.sub(r"\[.*\]", "", _str)]

    return " ".join(new_strs)


asyncio.run(main())
