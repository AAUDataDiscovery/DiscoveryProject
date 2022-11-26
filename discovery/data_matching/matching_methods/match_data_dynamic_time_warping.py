"""
Calculate the normalized distance measure between two numerical columns using Dynamic Time Warping
"""

from dtaidistance import dtw
import pandas as pd
import numpy
from discovery.data_matching.data_match_interface import DataMatcher


class MatchDataDynamicTimeWarping(DataMatcher):
    @staticmethod
    def run_process(series1: pd.Series = None, series2: pd.Series = None, **kwargs):
        max_distance = len(series1) * max(series1) / 100

        if pd.api.types.is_numeric_dtype(series1) and pd.api.types.is_numeric_dtype(series2):
            # distance = dtw.distance(series1, series2)
            distance = dtw.distance_fast(series1.to_numpy(dtype=numpy.double), series2.to_numpy(dtype=numpy.double))
            print(f"{distance} {max_distance}")
            return (max_distance - distance) / max_distance * 100
        return 0
