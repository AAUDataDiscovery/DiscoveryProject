"""
Data storage in memory
"""
from typing import Optional, Union

import pandas


class ColMetadata:
    name: str
    col_type: str
    mean: Union[int, float, None]
    min: any
    max: any

    def __init__(self, name: str, col_type: str, mean: Union[int, float, None], min_val, max_val):
        self.name = name
        self.col_type = col_type
        self.mean = mean
        self.min = min_val
        self.max = max_val


class Metadata:
    filepath: str
    columns: [ColMetadata]

    def __init__(self, filepath: str, columns: [ColMetadata]):
        self.filepath = filepath
        self.columns = columns
