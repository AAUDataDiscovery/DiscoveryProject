import pandas as pd

from dataframe_match_module.type_matchers.type_matcher import TypeMatcher


class TemperatureTypeMatcher(TypeMatcher):
    def is_recognized_series(self, series: pd.Series) -> bool:
        return all(series.apply(self.is_recognized_value))

    #TODO: come up with a more elegant solution or at least clean up and refactor to multiple functions
    def get_normalized_values(self, series: pd.Series) -> [pd.Series]:
        returnSeries = []
        if all(series.apply(self._is_valid_fahrenheit)):
            converted_fahrenheit = self._convert_series_fahrenheit_to_celsius(series)
            if all(converted_fahrenheit.apply(self._is_valid_celsius)):
                returnSeries.append(converted_fahrenheit)

        if all(series.apply(self._is_valid_kelvin)):
            converted_kelvin = self._convert_series_kelvin_to_celsius(series)
            if all(converted_kelvin.apply(self._is_valid_celsius)):
                returnSeries.append(converted_kelvin)

        if all(series.apply(self._is_valid_celsius)):
            returnSeries.append(series.apply(float))

        return returnSeries

    def is_recognized_value(self, value: float) -> bool:
        return self._is_valid_celsius(value) or\
               self._is_valid_fahrenheit(value) or\
               self._is_valid_kelvin(value)

    def _is_valid_celsius(self, value: float) -> bool:
        return -273.15 <= value  # highest temperature is infinity for practical purposes

    def _is_valid_fahrenheit(self, value: float) -> bool:
        return -459.67 <= value

    def _is_valid_kelvin(self, value: float) -> bool:
        return 0 <= value

    def _convert_fahrenheit_to_celsius (self, value: float) -> float:
        return float((value - 32) * (5/9))

    def _convert_kelvin_to_celsius (self, value: float) -> float:
        return float(value - 273)

    def _convert_series_fahrenheit_to_celsius (self, series: pd.Series) -> pd.Series:
        return series.apply(self._convert_fahrenheit_to_celsius)

    def _convert_series_kelvin_to_celsius (self, series: pd.Series) -> pd.Series:
        return series.apply(self._convert_kelvin_to_celsius)
