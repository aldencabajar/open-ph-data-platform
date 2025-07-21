import sys
import asyncio
from playwright.async_api import async_playwright

WIKIPEDIA_LINK = "https://en.wikipedia.org/wiki/List_of_cities_in_the_Philippines"
headers = ["City", "Population", "Area"]

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()

        page = await browser.new_page()

        await page.goto(WIKIPEDIA_LINK)

        table = page.locator("table").filter(has_text="Population")
        headers = await table.locator("thead tr th").filter(has_not=page.get_by_role("list")).all_inner_texts()
        rows = table.locator("tbody tr")

        table_data = await rows.evaluate_all("""(rowElements) => {
        return rowElements.map(row => {
            return Array.from(row.querySelectorAll('td')).map(cell => cell.textContent?.trim());
        });
    }""")

        # print(table_data)
        print(headers)
    
asyncio.run(main())
        