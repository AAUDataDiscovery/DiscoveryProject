"""
Data storage in memory
"""
from typing import Union

from abc import ABC
import pandas
from statsmodels.tsa.stattools import adfuller

from discovery.utils.metadata_enums import FileSizeUnit, FileExtension

# Manually set the tags on the existing datasets for now
TAGS_MAP = {
    'co2_emission_by_countries': ['health', 'weather and climate', 'environment', 'pollution'],
    'unsdg_2002_2021': ['economics', 'government'],
    'world_happiness_report_2015': ['arts and entertainment', 'news', 'social science',
                                    'religion and belief systems', 'economics'],
    'world_happiness_report_2016': ['arts and entertainment', 'news', 'social science',
                                    'religion and belief systems', 'economics'],
    'world_happiness_report_2017': ['arts and entertainment', 'news', 'social science',
                                    'religion and belief systems', 'economics'],
    'world_happiness_report_2018': ['arts and entertainment', 'news', 'social science',
                                    'religion and belief systems', 'economics'],
    'world_happiness_report_2019': ['arts and entertainment', 'news', 'social science',
                                    'religion and belief systems', 'economics'],
}


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
                 size: (int, FileSizeUnit), file_hash: int, no_of_rows: int,
                 columns: [] = [], tags: [] = []):
        self.file_path = file_path
        self.extension = extension
        self.size = size
        self.hash = int(file_hash)
        self.no_of_rows = no_of_rows
        self.columns = columns
        self.tags = tags


def construct_metadata_from_file_descriptor(file_descriptor):
    metadatum = Metadata(file_descriptor["file_path"], file_descriptor["extension"],
                         file_descriptor["size"], file_descriptor["file_hash"], file_descriptor["dataframe"].shape[0])
    col_meta = []
    dataframe = file_descriptor["dataframe"]

    for col_name in dataframe.columns:
        column_data = construct_column(dataframe[col_name])
        col_meta.append(column_data)
    metadatum.columns = col_meta
    metadatum.tags = TAGS_MAP[file_descriptor["dataframe_name"]]
    return metadatum


def construct_column(column):
    is_numeric_probability, average, col_min, col_max, continuity, stationarity = get_col_statistical_values(column)
    return NumericColMetadata(column.name, column.dtype, is_numeric_probability, continuity, average, col_min, col_max,
                              stationarity)


def get_col_statistical_values(column):
    numerified_column = numerify_column(column)

    is_numeric_probability = column_numeric_percentage(column)
    is_numeric = is_numeric_probability > 0.05

    col_min = column.min()
    col_max = column.max()

    continuity = column_is_continuous_probability(column)

    stationarity = is_column_stationary(numerified_column) if is_numeric else None

    average = numerified_column.mean() if is_numeric else None

    return is_numeric_probability, average, col_min, col_max, continuity, stationarity


def numerify_column(series):
    """
    Transform all values in a column to a numeric data type. Values that can't be transformed will be removed
    :param series:
    :return:
    """
    return pandas.to_numeric(series, 'coerce').dropna()


def column_numeric_percentage(series):
    """
    Determine the percentage of numeric values in a column
    :param series:
    :return:
    """
    return len(numerify_column(series)) / len(series)


def is_column_stationary(series):
    """
    Checks if the data in a column is stationary using the Dickey-Fuller test
    :param series:
    :return:
    """
    result = adfuller(series)
    return result[0] < result[4]['5%']


def column_is_continuous_probability(series):
    """
    Checks if the data in a column is continuous or categorical
    :param series:
    :return:
    """
    return series.nunique() / series.count()
