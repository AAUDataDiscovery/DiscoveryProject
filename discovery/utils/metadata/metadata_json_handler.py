import json
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_string_dtype
import numbers

from discovery.utils.metadata.metadata import Metadata, ColMetadata, Relationship

def write_metadata_to_json(metadata: Metadata):
    dictionary_representation = get_metadata_dictionary_representation(metadata)
    json_string = json.dumps(dictionary_representation)
    write_json_to_file(dictionary_representation["file_path"], json_string)


def get_metadata_dictionary_representation(metadata: Metadata):
    dictionary_representation = {
        "file_path": normalize_and_get_filepath(metadata.file_path),
        "extension": normalize_and_get_extension(metadata.extension),
        "size": normalize_and_get_size(metadata.size),
        "hash": normalize_and_get_hash(metadata.hash),
        "no_of_rows": metadata.no_of_rows,
        "tags": metadata.tags,
        "columns": get_columns_dictionary_representation(metadata.columns.values())
    }
    return dictionary_representation


def get_columns_dictionary_representation(columns: [ColMetadata]):
    columns_list = []
    for column in columns:
        column_dict = {
            "name": normalize_and_get_column_name(column.name),
            "is_numeric_percentage": column.is_numeric_percentage,
            "continuity": column.continuity,
            "mean": normalize_and_get_column_mean(column.mean),
            "minimum": normalize_and_get_column_min_max(column.minimum),
            "maximum": normalize_and_get_column_min_max(column.maximum),
            "stationarity": 1 if column.stationarity else 0,
            "relationships": get_relationships_dictionary_representation(column.relationships)
        }
        if column.columns is not None:
            column_dict["columns"] = get_columns_dictionary_representation(column.columns)
        columns_list.append(column_dict)
    return columns_list


def get_relationships_dictionary_representation(relationships: [Relationship]):
    relationships_list = []
    for relationship in relationships:
        relationships_list.append(
            {
                "certainty": str(relationship.certainty),
                "target_file_hash": str(relationship.target_file_hash),
                "target_column_name": normalize_and_get_column_name(relationship.target_column_name)
            }
        )
    return relationships_list


def write_json_to_file(path, json_string):
    filename = '{}.metadata.json'.format(path)
    with open(filename, 'w') as file:
        file.write(json_string)


def normalize_and_get_filepath(file_path):
    return file_path


def normalize_and_get_extension(extension):
    return extension.value


def normalize_and_get_size(size):
    return {
        "quantity": size[0],
        "unit": size[1].name
    }


def normalize_and_get_hash(file_hash):
    return file_hash


def normalize_and_get_column_name(name):
    return str(name)


def normalize_and_get_column_mean(mean):
    return normalize_pandas_numeric_values(mean)


def normalize_and_get_column_min_max(value):
    if is_string_dtype(value) or type(value) is str:
        return str(value)
    return normalize_pandas_numeric_values(value)


def normalize_pandas_numeric_values(value):
    if type(value) is float or type(value) is int:
        return value
    if is_numeric_dtype(value):
        return float(value)
    return None


def normalize_and_get_relationship_certainty(certainty):
    if isinstance(certainty, numbers.Number):
        return float(certainty)
    return None
