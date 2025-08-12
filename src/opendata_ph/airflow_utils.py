import os
from typing import List


def create_dag_id(filepath: str):
    return os.path.splitext(os.path.basename(filepath))[0]


def create_bash_python_command(path_to_script: str, args: List[str] = []):
    return (
        ".venv/bin/python "
        f"{path_to_script} "
        f"{" ".join([f'{arg}' for arg in args])}"
    )
