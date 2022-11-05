"""
Data storage in memory
"""
from typing import Union

from abc import ABC, abstractmethod
from pandas.api.types import is_numeric_dtype

from discovery.utils.metadata_enums import FileSizeUnit, FileExtension
from utils.dataframe_matcher import DataFrameMatcher


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

    def __init__(self, name: str, col_type: str, continuity: float, columns=None):
        self.name = name
        self.col_type = col_type
        self.continuity = continuity
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

    def __init__(self, name: str, col_type: str, is_numeric_percentage: float, continuity: float,
                 mean: Union[int, float, None], min_val, max_val,
                 stationarity: bool, columns=None):
        ColMetadata.__init__(self, name, col_type, continuity, columns)
        self.is_numeric_percentage = is_numeric_percentage
        self.mean = mean
        self.minimum = min_val
        self.maximum = max_val
        self.stationarity = stationarity


class CategoricalColMetadata(ColMetadata):
    def __init__(self, name: str, col_type: str, continuity: float, columns=None):
        ColMetadata.__init__(self, name, col_type, continuity)


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
    is_numeric_probability, average, col_min, col_max, continuity, stationarity = get_col_statistical_values(column)
    return NumericColMetadata(column.name, column.dtype, is_numeric_probability, continuity, average, col_min, col_max,
                              stationarity)


def get_col_statistical_values(column):
    numerified_column = DataFrameMatcher.numerify_column(column)

    is_numeric_probability = DataFrameMatcher.column_numeric_percentage(column)
    is_numeric = is_numeric_probability >= 0.5

    col_min = column.min()
    col_max = column.max()

    continuity = DataFrameMatcher.column_is_continuous_probability(column)

    stationarity = DataFrameMatcher.is_column_stationary(numerified_column) if is_numeric else None

    average = numerified_column.mean() if is_numeric else None

    return is_numeric_probability, average, col_min, col_max, continuity, stationarity
