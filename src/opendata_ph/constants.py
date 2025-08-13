from enum import StrEnum
class OpenDataPHConstants:
    BUILD_FOLDER_VAR = "build_folder"
    DUCKLAKE_METADATA_CONN = "ducklake_metadata_conn"
    DUCKDB_PROCESS_POOL = "duckdb_process_pool"

class DataLakeLayers(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
