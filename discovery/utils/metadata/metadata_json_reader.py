import json
import os
from jsonschema import validate
from discovery.utils.metadata.metadata import Metadata, NumericColMetadata, Relationship
from discovery.utils.metadata.metadata_enums import FileSizeUnit, FileExtension


def verify_json(json: dict):
    schema_path = os.path.dirname(os.path.abspath(__file__)) + "/schema.json"

    schema = load_file_into_dict(schema_path)
    validate(instance=json, schema=schema)


def read_metadata_from_json(path: str, verify: bool = True):
    json_dict = load_file_into_dict(path)
    if verify:
        verify_json(json_dict)
    return convert_dict_metadata_obj(json_dict)


def convert_dict_metadata_obj(json_dict):
    metadata = Metadata(
        file_path=json_dict["file_path"],
        extension=FileExtension(json_dict["extension"]),
        datagen=None,
        size=(json_dict["size"]["quantity"],  FileSizeUnit(json_dict["size"]["unit"])),
        file_hash=json_dict["hash"],
        no_of_rows=json_dict["no_of_rows"],
        columns={},
        tags=json_dict["tags"]
    )
    for col in json_dict["columns"]:
        metadata.columns[col["name"]] = convert_dict_to_col(col)
    return metadata

def convert_dict_to_col(col_dict):
    column = NumericColMetadata (
        name=col_dict["name"],
        col_type="",
        is_numeric_percentage=col_dict["is_numeric_percentage"],
        continuity=col_dict["continuity"],
        mean=col_dict["mean"],
        min_val=col_dict["minimum"],
        max_val=col_dict["maximum"],
        stationarity=col_dict["stationarity"] == 1,
    )
    for relationship in col_dict["relationships"]:
        column.relationships.append(convert_dict_to_relationship(relationship))

    return column

def convert_dict_to_relationship(relationship_dict):
     return Relationship(
         certainty=relationship_dict["certainty"],
         target_file_hash=relationship_dict["target_file_hash"],
         target_column_name=relationship_dict["target_column_name"]
    )

def load_file_into_dict(path):
    with open(path, 'r') as f:
        content = json.load(f)
    return content
