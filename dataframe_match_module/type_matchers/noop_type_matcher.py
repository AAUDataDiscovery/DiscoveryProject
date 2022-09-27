import pandas as pd

from dataframe_match_module.type_matchers.type_matcher import TypeMatcher


class NoOpTypeMatcher(TypeMatcher):
    def is_recognized_series(self, series: pd.Series) -> bool:
        return True

    def get_normalized_values(self, series: pd.Series) -> [[]]:
        return [series]

    def is_recognized_value(self, value: float) -> bool:
        return True
