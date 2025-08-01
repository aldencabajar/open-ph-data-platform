import duckdb


def initialize_duckdb_catalog(
    ducklake_catalog_conn: str, catalog_name: str = "dl"
) -> str:
    duckdb.sql("INSTALL sqlite; INSTALL ducklake;")

    duckdb.sql(f"ATTACH 'ducklake:sqlite:{ducklake_catalog_conn}' AS {catalog_name}")

    return catalog_name
