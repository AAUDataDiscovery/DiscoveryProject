"""
Takes a reference DataFrame and a subject one and tries to match the columns of the subject to the reference
"""
import pandas
import logging

logger = logging.getLogger(__name__)


class DataFrameMatcher:
    def __init__(self, reference_df, subject_df):
        self.reference_df = reference_df
        self.subject_df = subject_df

    def match_dataframes(self):
        """
        Displays the columns of the subject DF with their possible matches in the reference DF
        :return:
        """
        pass

    @staticmethod
    def _match_columns(first, second):
        """
        Produces a percentage of similarity between 2 columns
        :param first:
        :param second:
        :return:
        """
        pass
