from abc import abstractmethod

import pandas as pd


class TypeMatcher:
    """base class for all type matchers,
    probably will be broken down to multiple classes in the future"""

    @abstractmethod
    def is_recognized_value(self, value: any) -> bool:
        """returns wheter the implementing class can handle the value"""

    @abstractmethod
    def is_recognized_series(self, value: any) -> bool:
        """returns wheter the implementing class can handle the all values in the series"""

    @abstractmethod
    def get_normalized_values(self, series: pd.Series) -> [[]]:
        """returns a matrix because some data might be interpreted in multiple different ways,
        for example 30 degrees can be celsius or kelvin """
