"""
Takes two dataframes and tries to match them column by column
"""
import logging

# TODO: currently not working out of the box, must be run with the following statements...
# >>> import nltk
# >>> nltk.download('wordnet')
# >>> nltk.download('omw-1.4')
import numpy as np
import pandas
import numpy
from difflib import SequenceMatcher
from Levenshtein import ratio
from nltk.corpus import wordnet
from itertools import product
from dtaidistance import dtw
import scipy.stats as stats
import numbers

from discovery.utils.metadata import metadata
from discovery.utils.metadata.metadata import Metadata, NumericColMetadata

logger = logging.getLogger(__name__)


class DataFrameMatcher:
    def __init__(self, methods):
        self.methods = methods

    @staticmethod
    def match_dataframes(self,
                         metadatum1: Metadata, df1: pandas.DataFrame,
                         metadatum2: Metadata, df2: pandas.DataFrame):
        """
        Returns the best column match in df2 for each column in df1 with scored and confidence percentages
        :return:
        """

        results = []

        return results

    @staticmethod
    def match_columns(dataframe1, column1_name, dataframe2, column2_name):
        """
        Calculate the similarity of 2 columns using a combination of methods
        :param column2_name:
        :param dataframe2:
        :param column1_name:
        :param dataframe1:
        :return:
        """

        similarity = 0
        no_of_methods_used = 0

        # Match by column names
        similarity += DataFrameMatcher.match_name_lcs(column1_name, column2_name)
        similarity += DataFrameMatcher.match_name_levenshtein(column1_name, column2_name)
        no_of_methods_used = 2

        # Match by values
        series1 = dataframe1.loc[:, column1_name]
        series2 = dataframe2.loc[:, column2_name]

        similarity += DataFrameMatcher.match_data_identical_values(series1, series2)
        no_of_methods_used += 1

        # Match by numerical algorithms
        numerified_series1 = metadata.numerify_column(series1)
        numerified_series2 = metadata.numerify_column(series2)

        if metadata.column_numeric_percentage(series1) > 0.05 and \
                metadata.column_numeric_percentage(series2) > 0.05:
            similarity += DataFrameMatcher.match_data_pearson_coefficient(numerified_series1, numerified_series2)
            # similarity += DataFrameMatcher.match_data_dynamic_time_warping(numerified_series1, numerified_series2)
            # similarity += DataFrameMatcher.match_data_two_sample_t_test(numerified_series1, numerified_series2)
            no_of_methods_used += 1

        # print(f"{column1_name} {column2_name} {similarity / no_of_methods_used}")

        return similarity / no_of_methods_used

    @staticmethod
    def match_column_in_dataframe(df1: pandas.DataFrame, column1: NumericColMetadata,
                                  metadata2: Metadata, df2: pandas.DataFrame):
        """
        Find the best match of a column in another dataframe
        :param df1:
        :param column1:
        :param metadata2:
        :param df2:
        :return:
        """
        scores = {}
        no_of_tests = {}
        average_differences = {}
        minimum_differences = {}
        maximum_differences = {}
        for column2 in metadata2.columns:
            # LCS name test
            lcs_percentage = DataFrameMatcher.match_name_lcs(column1.name, column2.name)

            # Levenshtein name test
            levenshtein_percentage = DataFrameMatcher.match_name_levenshtein(column1.name, column2.name)

            # Data type test
            data_type_matches = 100 if column1.col_type == column2.col_type else 0

            # Continuity test
            continuity_percentage = 100 * (1 - abs(column1.continuity - column2.continuity))

            # Numerical values test
            numerical_percentage = 100 * (1 - abs(column1.is_numeric_percentage -
                                                  column2.is_numeric_percentage))

            # Average test
            if column1.mean is not None and column2.mean is not None:
                average_differences[column2.name] = abs(column1.mean - column2.mean)

            # Min test
            try:
                minimum1 = float(column1.minimum)
                minimum2 = float(column2.minimum)
                minimum_differences[column2.name] = abs(minimum1 - minimum2)
            except ValueError:
                pass

            # Max test
            try:
                maximum1 = float(column1.maximum)
                maximum2 = float(column2.maximum)
                maximum_differences[column2.name] = abs(maximum1 - maximum2)
            except ValueError:
                pass

            # Average similarity
            average_similarity = lcs_percentage + levenshtein_percentage + data_type_matches + \
                                 continuity_percentage + numerical_percentage

            scores[column2.name] = average_similarity
            no_of_tests[column2.name] = 5

        # Normalize the average differences
        normalized_average_differences = DataFrameMatcher.normalize_values(average_differences)
        for name, similarity in normalized_average_differences.items():
            scores[name] += similarity
            no_of_tests[name] += 1

        # Normalize the minimum differences
        normalized_minimum_differences = DataFrameMatcher.normalize_values(minimum_differences)
        for name, similarity in normalized_minimum_differences.items():
            scores[name] += similarity
            no_of_tests[name] += 1

        # Normalize the maximum differences
        normalized_maximum_differences = DataFrameMatcher.normalize_values(maximum_differences)
        for name, similarity in normalized_maximum_differences.items():
            scores[name] += similarity
            no_of_tests[name] += 1

        for column2 in metadata2.columns:
            scores[column2.name] /= no_of_tests[column2.name]

        best_similarity = 0
        best_name = ''
        for name, similarity in scores.items():
            if similarity > best_similarity:
                best_similarity = similarity
                best_name = name

        return best_name, best_similarity

    @staticmethod
    def normalize_values(dictionary: dict):
        normalized_dictionary = {}
        min_value = numpy.inf
        max_value = 0
        for index, value in dictionary.items():
            if value < min_value:
                min_value = value
            if value > max_value:
                max_value = value

        for index, value in dictionary.items():
            normalized_dictionary[index] = (1 - (value - min_value) / (max_value - min_value)) * 100

        return normalized_dictionary

    @staticmethod
    def match_data_identical_values(column_a, column_b):
        """
        Calculate a similarity percentage with set operations (intersection vs union) for (possible) categorical data
        :param column_a:
        :param column_b:
        :return:
        """

        stringified_column_a = column_a.apply(str)
        stringified_column_b = column_b.apply(str)

        categorical_similarity = len(numpy.intersect1d(stringified_column_a, stringified_column_b)) / len(
            numpy.union1d(stringified_column_a, stringified_column_b)) * 100
        return categorical_similarity

    @staticmethod
    def match_data_pearson_coefficient(column_a, column_b):
        """
        Calculate the Pearson correlation coefficient between the 2 columns if they are both numerical
        :param column_a:
        :param column_b:
        :return:
        """

        pearson_correlation_coefficient = abs(column_a.corr(column_b)) * 100
        return pearson_correlation_coefficient

    @staticmethod
    def match_data_dynamic_time_warping(column_a, column_b):
        """
        Calculate the normalized distance measure between two numerical columns using Dynamic Time Warping
        :param column_a:
        :param column_b:
        :return:
        """

        max_distance = len(column_a) * max(column_a)

        if pandas.api.types.is_numeric_dtype(column_a) and pandas.api.types.is_numeric_dtype(column_b):
            return (max_distance - dtw.distance(column_a, column_b)) / max_distance * 100
        return 0

    @staticmethod
    def match_data_two_sample_t_test(column_a, column_b):
        """
        Determine if the unknown population means of the two groups are equal
        :param column_a:
        :param column_b:
        :return:
        """

        # Normalize the data
        column_a = column_a / np.max(np.abs(column_a), axis=0)
        column_b = column_b / np.max(np.abs(column_b), axis=0)

        # First, we determine if the 2 groups have the same variance
        # A ratio of less than 4:1 indicates we should consider the variances equal
        variance_a = np.var(column_a)
        variance_b = np.var(column_b)
        if min(variance_a, variance_b) == 0:
            equal_variances = max(variance_a, variance_b) < 4
        else:
            equal_variances = (max(variance_a, variance_b) / min(variance_a, variance_b)) < 4

        # Perform the two sample T-test
        result_statistic, result_p_value = stats.ttest_ind(column_a, column_b, equal_var=equal_variances)

        # If the p-value is greater than 0.05, we accept the null hypothesis that the mean of the two groups is equal
        # Otherwise, we reject it, and claim that the means are different
        return result_p_value > 0.05

    @staticmethod
    def match_name_lcs(name_a, name_b):
        """
        Calculate the similarity of 2 column names using the Longest Contiguous Matching Subsequence
        :param name_a:
        :param name_b:
        :return:
        """

        return SequenceMatcher(None, name_a, name_b).ratio() * 100

    @staticmethod
    def match_name_levenshtein(name_a, name_b):
        """
        Calculate the similarity of 2 column names using the Levenshtein distance
        :param name_a:
        :param name_b:
        :return:
        """

        return ratio(name_a, name_b) * 100

    @staticmethod
    def match_name_wordnet(name_a, name_b):
        """
        Calculate the similarity of 2 column names using WordNet
        :param name_a:
        :param name_b:
        :return:
        """

        synset_first = wordnet.synsets(name_a)
        synset_second = wordnet.synsets(name_b)
        wordnet_ratio = 0
        if len(synset_first) > 0 and len(synset_second) > 0:
            wordnet_ratio = max(
                wordnet.wup_similarity(s1, s2) for s1, s2 in product(synset_first, synset_second)) * 100

        return wordnet_ratio
