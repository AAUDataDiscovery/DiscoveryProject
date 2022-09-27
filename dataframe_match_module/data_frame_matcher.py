import pandas as pd

from dataframe_match_module.type_matchers.type_matcher import TypeMatcher

"""This is a helper class that may find matching columns across independent dataframes"""


class DataFrameMatcher:
    type_matchers: [TypeMatcher]

    def __init__(self, type_matchers: [TypeMatcher]):
        self.type_matchers = type_matchers

    def match_all_columns(self, df1: pd.DataFrame, df2: pd.DataFrame) -> object:
        """matches all columns to all other columns, and returns a list of tuples with the matching columns"""

        # TODO: make it cleaner
        matches = []
        for df1Col in df1.columns:
            for df2Col in df2.columns:
                if self._check_cols_against_matchers(df1[df1Col], df2[df2Col]):
                    matches.append(self._create_col_tuple(df1Col, df2Col))
        return matches

    def _check_if_cols_equal(self, col1: pd.Series, col2: pd.Series):
        return col1.equals(col2)

    def _check_cols_against_matchers(self, col1: pd.Series, col2: pd.Series):
        # TODO: make it cleaner
        for matcher in self.type_matchers:
            for col1_normalized_option in matcher.get_normalized_values(col1):
                for col2_normalized_option in matcher.get_normalized_values(col2):
                    if self._check_if_cols_equal(col1_normalized_option, col2_normalized_option):
                        return True

    def _create_col_tuple(self, col1: pd.DataFrame.columns, col2: pd.DataFrame.columns) -> object:
        return col1, col2
