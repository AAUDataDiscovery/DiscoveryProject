"""
Data storage in memory
"""
from typing import Union

from abc import ABC, abstractmethod
from pandas.api.types import is_numeric_dtype

from discovery.utils.metadata_enums import FileSizeUnit, FileExtension


class Relationship:
    certainty: int
    target_file_hash: int
    target_column_name: str

    def __init__(self, certainty, target_file_hash, target_column_name):
        self.certainty = certainty
        self.target_file_hash = target_file_hash
        self.target_column_name = target_column_name


class ColMetadata(ABC):
    name: str
    col_type: str
    columns: any  # [ColMetadata]
    relationships: [Relationship]

    def __init__(self, name: str, col_type: str, columns=None):
        self.name = name
        self.col_type = col_type
        self.columns = columns
        self.relationships = []

    def set_columns(self, columns):
        self.columns = columns

    def add_relationship(self, certainty, target_file_hash, target_column_name):
        self.relationships.append(Relationship(certainty, target_file_hash, target_column_name))


class NumericColMetadata(ColMetadata):
    mean: Union[float, None]
    minimum: any
    maximum: any

    def __init__(self, name: str, col_type: str, mean: Union[int, float, None], min_val, max_val, columns=None):
        ColMetadata.__init__(self, name, col_type, columns)
        self.mean = mean
        self.minimum = min_val
        self.maximum = max_val


class CategoricalColMetadata(ColMetadata):
    def __init__(self, name: str, col_type: str, columns=None):
        ColMetadata.__init__(self, name, col_type, columns)


class Metadata:
    def __init__(self, file_path: str, extension: FileExtension,
                 size: (int, FileSizeUnit), file_hash: int,
                 columns: [] = []):
        self.file_path = file_path
        self.extension = extension
        self.size = size
        self.hash = int(file_hash)
        self.columns = columns


def construct_metadata_from_file_descriptor(file_descriptor):
    metadatum = Metadata(file_descriptor["file_path"], file_descriptor["extension"],
                         file_descriptor["size"], file_descriptor["file_hash"])
    col_meta = []
    dataframe = file_descriptor["dataframe"]

    for col_name in dataframe.columns:
        column_data = construct_column(dataframe[col_name])
        col_meta.append(column_data)
    metadatum.columns = col_meta
    return metadatum


def construct_column(column):
    average, col_min, col_max = get_col_statistical_values(column)
    return NumericColMetadata(column.name, column.dtype, average, col_min, col_max)


def get_col_statistical_values(column):
    average, col_min, col_max = None, None, None

    col_min = column.min()
    col_max = column.max()

    if is_numeric_dtype(column):
        average = column.mean()
    return average, col_min, col_max
