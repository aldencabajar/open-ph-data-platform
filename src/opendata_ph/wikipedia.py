from typing import List


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
            continue

        is_multi_header_row = False
        for header in splitted_text:
            if header == "":
                if is_multi_header_row:
                    temp_headers.pop(0)
                    is_multi_header_row = False
                headers.append(temp_headers.pop(0))
            else:
                # we set a temp text flag here to denote 
                # that we are currently seeing multiple header rows
                is_multi_header_row = True
                headers.append(temp_headers[0] + "_" + header)
        
    return headers
