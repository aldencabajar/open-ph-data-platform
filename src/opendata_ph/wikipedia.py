from pandas import DataFrame
from datetime import datetime
from typing import Callable, List
from urllib.parse import unquote, urlparse

from playwright.async_api import async_playwright, Locator


def merge_multiple_header_rows(header_texts: List[str]) -> List[str]:
    """Merges multiple header rows from a wikipedia table.

    Args:
        header_texts (List[str]): List of unsplitted header rows.

    Returns:
        List[str]: List of merged headers.
    """
    # if number of table rows is greater than 1, this means
    # that it is a multi-row header.
    headers = []
    temp_headers = []
    for text in header_texts:
        splitted_text = text.split("\t")
        if not temp_headers:
            temp_headers += splitted_text
            headers += splitted_text
            continue

        headers = []
        is_multi_header_col = False
        for header in splitted_text:
            if header == "":
                if is_multi_header_col:
                    temp_headers.pop(0)
                    is_multi_header_col = False
                headers.append(temp_headers.pop(0))
            else:
                # we set a temp text flag here to denote
                # that we are currently seeing multiple header rows
                is_multi_header_col = True
                headers.append(temp_headers[0] + "_" + header)
        temp_headers = headers

    return headers


async def get_last_edit_timestamp(page_url: str) -> datetime:
    async with async_playwright() as p:
        # Create API request context
        api_context = await p.request.new_context(
            base_url="https://en.wikipedia.org/w/api.php"
        )

        page_title = wikipedia_title_from_url(page_url)

        # Make the API request
        response = await api_context.get(
            "",
            params={
                "action": "query",
                "titles": page_title,
                "prop": "revisions",
                "rvprop": "timestamp",
                "format": "json",
                "formatversion": "2",
            },
        )

        if not response.ok:
            raise Exception(f"Request failed: {response.status} {response.status_text}")

        data = await response.json()
        page_data = data["query"]["pages"][0]

        await api_context.dispose()

        last_edit_timestamp = page_data["revisions"][0]["timestamp"]
        return datetime.fromisoformat(last_edit_timestamp)


def wikipedia_title_from_url(url: str) -> str:
    path = urlparse(url).path  # e.g., "/wiki/Albert_Einstein"
    if path.startswith("/wiki/"):
        encoded_title = path[len("/wiki/") :]
        return unquote(
            encoded_title.replace("_", " ")
        )  # decode and replace underscores
    else:
        raise ValueError("Not a valid Wikipedia article URL")


async def scrape_wikipedia_table(
    table_locator: Locator, header_cleaner_func: Callable[[str], str] | None = None
) -> DataFrame:
    # Extract headers
    header_elements = await table_locator.locator("thead tr").all_inner_texts()
    headers = merge_multiple_header_rows(header_elements)

    # Extract rows
    row_elements = table_locator.locator("tbody tr")
    table_data = await row_elements.evaluate_all(
        """(rowElements) => {
        return rowElements.map(row => {
            return Array.from(row.querySelectorAll('th, td')).map(cell => cell.textContent?.trim());
        });
    }"""
    )

    if header_cleaner_func:
        headers = list(
            filter(lambda header: header != "", map(header_cleaner_func, headers))
        )

    # Create DataFrame
    df = DataFrame(table_data, columns=headers)

    return df
