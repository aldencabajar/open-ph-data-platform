import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from pydantic import BaseModel, Field


class ObjectMetadata(BaseModel):
    description: str
    tags: List[str] = Field(default=[])
    retrieved_timestamp: datetime = Field(alias="retrievedTimestamp")
    source_uri: str = Field(alias="sourceUri")
    source_timestamp: datetime = Field(alias="sourceTimestamp")


class RawFilesMetadata(BaseModel):
    description: str
    objects: Dict[str, ObjectMetadata]


def load_metadata(metadata_file_path: Path) -> RawFilesMetadata:
    return RawFilesMetadata.model_validate(json.load(metadata_file_path.open()))


def get_object_metadata_by_tag(
    raw_files_metadata: RawFilesMetadata, tags: List[str]
) -> Dict[str, ObjectMetadata]:

    to_return = {
        rel_path: meta
        for rel_path, meta in raw_files_metadata.objects.items()
        if all([tag in meta.tags for tag in tags])
    }

    if not to_return:
        raise ValueError(f"No metadata found for tags: {tags}")

    return to_return


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
