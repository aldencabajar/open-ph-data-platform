import json
from datetime import datetime
from pathlib import Path
from typing import Dict

from pydantic import BaseModel, Field


class ObjectMetadata(BaseModel):
    description: str
    retrieved_timestamp: datetime = Field(alias="retrievedTimestamp")
    source_uri: str = Field(alias="sourceUri")
    source_timestamp: datetime = Field(alias="sourceTimestamp")


class RawFilesMetadata(BaseModel):
    description: str
    objects: Dict[str, ObjectMetadata]


def parse_metadata(metadata_file_path: Path, object_file_path: Path) -> ObjectMetadata:
    raw_files_metadata = RawFilesMetadata.model_validate(
        json.load(metadata_file_path.open())
    )

    for rel_path, meta in raw_files_metadata.objects.items():
        # it is assumed that the metadata_file_pat
        # sits at the parent folder

        path_to_compare = metadata_file_path.parent / rel_path

        if path_to_compare == object_file_path:
            return meta

    raise ValueError(f"No metadata found for {object_file_path}")
