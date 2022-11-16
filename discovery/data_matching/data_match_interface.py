"""
Implements standard methods expected of data matching techniques

NOTE:
    we're filling out unused parameters on purpose
TODO:
    add decorators for enforcing int values
"""
import pandas as pd
from discovery.utils.metadata.metadata import ColMetadata, Metadata


class DataMatcher:
    def __init__(self):
        self.name = self.__class__.__qualname__

    def match_columns(
            self,
            col_meta1: ColMetadata,
            col_meta2: ColMetadata,
            series1: pd.Series = None,
            series2: pd.Series = None,
            metadata1: Metadata = None,
            metadata2: Metadata = None

    ) -> int:
        incoming = locals()
        incoming.pop('self')
        return self.run_process(**incoming)

    @staticmethod
    def run_process(
            col_meta1: ColMetadata,
            col_meta2: ColMetadata,
            series1: pd.Series = None,
            series2: pd.Series = None,
            metadata1: Metadata = None,
            metadata2: Metadata = None
    ) -> int:
        """
        Run the actual implemented process
        Reiterate the match_columns parameters here, as IDE's can pick it up
        """
        raise NotImplementedError
