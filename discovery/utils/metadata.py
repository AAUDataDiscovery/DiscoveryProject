"""
Data storage in memory
"""
from typing import Optional

import pandas


class Metadata:
    filepath: str
    mean: Optional[int, float]
    min: any
    max: any

    def __init__(self, filepath: str, mean: Optional[int, float], min_val, max_val):
        self.filepath = filepath
        self.mean = mean
        self.min = min_val
        self.max = max_val

