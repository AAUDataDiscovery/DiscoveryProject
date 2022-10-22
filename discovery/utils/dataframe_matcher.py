"""
Takes two dataframes and tries
"""
import yaml
import pandas
import numpy
import logging.config
from difflib import SequenceMatcher
from Levenshtein import ratio
from nltk.corpus import wordnet
from itertools import product

with open('../logging_conf.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
logger = logging.getLogger(__name__)


class DataFrameMatcher:
    def __init__(self, name_matcher, data_matcher):
        self.name_matcher = name_matcher
        self.data_matcher = data_matcher

    def match_dataframes(self, df_a: pandas.DataFrame, df_b: pandas.DataFrame):
        """
        Returns all pairs of columns from dataframes A and B with their confidence percentages based on the column
        names, and on the column contents.
        :return:
        """

        similarities = []

        for column_a in df_a.columns:
            for column_b in df_b.columns:
                name_confidence = self.name_matcher(column_a, column_b)
                data_confidence = self.data_matcher(df_a.loc[:, column_a], df_b.loc[:, column_b])

                similarity = {
                    'column_a': column_a,
                    'column_b': column_b,
                    'name_confidence': name_confidence,
                    'data_confidence': data_confidence,
                }
                logger.debug(similarity)
                similarities.append(similarity)

        return similarities

    @staticmethod
    def match_data_identical_values(column_a, column_b):
        """
        Calculate a similarity percentage with set operations (intersection vs union) for (possible) categorical data
        :param column_a:
        :param column_b:
        :return:
        """

        categorical_similarity = len(numpy.intersect1d(column_a, column_b)) / len(
            numpy.union1d(column_a, column_b)) * 100
        return categorical_similarity

    @staticmethod
    def match_data_pearson_coefficient(column_a, column_b):
        """
        Calculate the Pearson correlation coefficient between the 2 columns if they are both numerical
        :param column_a:
        :param column_b:
        :return:
        """

        if pandas.api.types.is_numeric_dtype(column_a) and pandas.api.types.is_numeric_dtype(column_b):
            pearson_correlation_coefficient = abs(column_a.corr(column_b)) * 100
            return pearson_correlation_coefficient
        return 0

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
