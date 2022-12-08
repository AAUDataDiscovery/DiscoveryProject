from typing import Iterable

import pandas as pd
import pandas.errors

from utils.data_readers.catalogue_data import CatalogueData


class LocalCSVReader(CatalogueData):
    def __init__(self, path):
        """If we don't know the amount of rows that already exist, we have to read the entire dataframe upon init"""
        self.path = path

    def get_data(self, rows: int | None = None) -> pd.DataFrame | Iterable[pd.DataFrame]:
        """Return a generator for the data

        Use lower row counts to preserve memory, and higher row counts for more efficient reading
        Args:
            rows: The amount of rows to generate on each iteration

        Returns: A generator of the dataframe, returning the amount of rows specified each time

        """
        if rows is None:
            yield pd.read_csv(self.path)

        seek_row = 0
        header_df = pd.read_csv(self.path, skiprows=seek_row, nrows=rows)
        df_columns = header_df.columns
        yield header_df

        while True:
            seek_row += rows
            try:
                yield pd.read_csv(self.path, skiprows=seek_row, nrows=rows, usecols=df_columns)
            except pandas.errors.EmptyDataError:
                break

