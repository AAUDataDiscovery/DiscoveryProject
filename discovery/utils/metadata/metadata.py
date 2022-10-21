"""
Data storage in memory
"""
from typing import Optional, Union
from utils.metadata_enums import FileSizeUnit, FileExtension

import pandas
import json
import numbers


class Relationship:
    certainty: int
    target_file_hash: int
    target_column_name: str

    def __init__(self, certainty, target_file_hash, target_column_name):
        self.certainty = certainty
        self.target_file_hash = target_file_hash
        self.target_column_name = target_column_name


class ColMetadata:
    name: str
    col_type: str
    mean: Union[int, float, None]
    minimum: any
    maximum: any
    columns: any  # [ColMetadata]
    relationships: [Relationship]

    def __init__(self, name: str, col_type: str, mean: Union[int, float, None], min_val, max_val, columns=None):
        self.name = name
        self.col_type = col_type
        self.mean = mean
        self.minimum = min_val
        self.maximum = max_val
        self.columns = columns
        self.relationships = []

    def set_columns(self, columns):
        self.columns = columns

    def add_relationship(self, certainty, target_file_hash, target_column_name):
        self.relationships.append(Relationship(certainty, target_file_hash, target_column_name))

    def normalize_for_json(self):
        self.col_type = self._normalize_col_type(self.col_type)
        self.mean = self._normalize_mean_value(self.mean)
        self.minimum = self._normalize_minmax_values(self.minimum)
        self.maximum = self._normalize_minmax_values(self.maximum)
        if self.columns is not None:
            for column in self.columns:
                column.normalize_for_json()

    def _normalize_col_type(self, col):
        return str(col)

    def _normalize_mean_value(self, mean):
        if mean is not None:
            return float(mean)
        return None


    def _normalize_minmax_values(self, val):
        if val is not None:
            if isinstance(val, numbers.Number):
                return float(val)
        return None


class Metadata:
    def __init__(self, file_path: str, extension: FileExtension,
                 size: (int, FileSizeUnit), file_hash: int,
                 columns: [] = []):
        self.file_path = file_path
        self.extension = extension
        self.size = size
        self.hash = int(file_hash)
        self.columns = columns
