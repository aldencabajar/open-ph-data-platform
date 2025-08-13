import pytz
from datetime import datetime
import os
from typing import List

import duckdb


def create_dag_id(filepath: str):
    return os.path.splitext(os.path.basename(filepath))[0]


def check_if_table_exists(schema: str, table: str) -> bool:
    is_exists = duckdb.sql(
        f"""
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = '{table}' and table_schema = '{schema}'
    ) as exists;
    """
    ).to_df()["exists"][0]

    return is_exists


def check_if_table_is_stale(
    catalog: str,
    table: str,
    schema: str,
    timestamp_column: str,
    timestamp_to_compare: datetime,
) -> bool:

    max_load_ts: datetime = duckdb.sql(
        f"""
        SELECT max({timestamp_column}) as load_dt
        FROM {catalog}.{schema}.{table}
        """
    ).to_df()["load_dt"][0]

    max_load_ts = pytz.utc.localize(max_load_ts)

    if max_load_ts < timestamp_to_compare:
        return True

    return False


def create_bash_python_command(path_to_script: str, args: List[str] = []):
    return (
        ".venv/bin/python "
        f"{path_to_script} "
        f"{" ".join([f'{arg}' for arg in args])}"
    )
