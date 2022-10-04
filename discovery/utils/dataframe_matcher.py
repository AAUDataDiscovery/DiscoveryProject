"""
Takes a reference DataFrame and a subject one and tries to match the columns of the subject to the reference
"""
import pandas
import numpy
import logging
from difflib import SequenceMatcher
import statistics

logger = logging.getLogger(__name__)


class DataFrameMatcher:
    def __init__(self, reference_df: pandas.DataFrame, subject_df: pandas.DataFrame):
        self.reference_df = reference_df
        self.subject_df = subject_df

    def match_dataframes(self):
        """
        Displays the columns of the subject DF with their possible matches in the reference DF
        :return:
        """

        similarities = []
        for column in self.reference_df.columns:
            similarities.append({
                'column': column,
                'percentage': 0.0,
            })

        for column in self.subject_df.columns:
            print(column)

            for i in range(0, len(similarities)):
                similarities[i]['percentage'] = self._match_columns(similarities[i]['column'], column)

            similarities.sort(key=lambda x: x['percentage'], reverse=True)

            for item in similarities:
                print(f"\t{item['column']} {item['percentage']}%")

    def _match_columns(self, reference_column_name, subject_column_name):
        """
        Produces a percentage of similarity between 2 columns
        :param reference_column_name:
        :param subject_column_name:
        :return:
        """

        reference_column = self.reference_df.loc[:, reference_column_name]
        subject_column = self.subject_df.loc[:, subject_column_name]

        percentages = []

        # Check how closely the names of the columns are related
        name_similarity = SequenceMatcher(None, reference_column_name, subject_column_name).ratio() * 100
        percentages.append(name_similarity)

        # Check if the 2 columns have the same data type
        data_type_similarity = 100 if self.reference_df.dtypes[reference_column_name] == self.subject_df[subject_column_name] else 0
        percentages.append(data_type_similarity)

        # Calculate a similarity percentage with set operations (intersection vs union) for (possible) categorical data
        if not pandas.api.types.is_numeric_dtype(reference_column) and not pandas.api.types.is_numeric_dtype(subject_column):
            categorical_similarity = len(numpy.intersect1d(reference_column, subject_column)) / len(numpy.union1d(reference_column, subject_column)) * 100
            percentages.append(categorical_similarity)

        # Calculate the Pearson correlation coefficient between the 2 columns if they are both numerical
        if pandas.api.types.is_numeric_dtype(reference_column) and pandas.api.types.is_numeric_dtype(subject_column):
            pearson_correlation_coefficient = abs(reference_column.corr(subject_column)) * 100
            percentages.append(pearson_correlation_coefficient)

        # Produce a similarity percentage as an average of all checks
        return round(sum(percentages) / len(percentages), 2)
