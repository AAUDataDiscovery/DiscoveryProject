from typing import Generator, Callable

import pandas as pd
from pandas.util import hash_pandas_object


class CatalogueData:
    """Catalogue data abstraction which allows interaction with the data, ideally avoids storing data in memory"""

    def get_data(self, rows) -> Generator:
        raise NotImplemented

    @staticmethod
    def get_checksum(dataframe: pd.DataFrame) -> int:
        """Return a checksum for the given dataframe

        Allows for abstracting checksum calculations into the reader itself
        """
        return int(hash_pandas_object(dataframe).sum())
