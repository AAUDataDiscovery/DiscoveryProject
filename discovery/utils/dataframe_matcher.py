"""
Takes a reference DataFrame and a subject one and tries to match the columns of the subject to the reference
"""
import pandas
import numpy
import logging
from difflib import SequenceMatcher
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from Levenshtein import ratio
from nltk.corpus import wordnet
from itertools import product

logger = logging.getLogger(__name__)


class DataFrameMatcher:
    def __init__(self, name_matcher, data_matcher):
        self.name_matcher = name_matcher
        self.data_matcher = data_matcher

    def match_dataframes(self, df_a, df_b):
        """
        Returns all pairs of columns from dataframes A and B with their confidence percentages based on the column
        names, and on the column contents.
        :return:
        """

        similarities = []
        for column in df_b.columns:
            similarities.append({
                'column': column,
                'percentage': 0.0,
            })

        for column_a in df_a.columns:
            for column_b in df_b.columns:
                name_confidence = 0
                data_confidence = 0

                similarities.append({
                    'column_a': column_a,
                    'column_b': column_b,
                    'name_confidence': name_confidence,
                    'data_confidence': data_confidence,
                })

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
        data_type_similarity = 100 if self.reference_df.dtypes[reference_column_name] == self.subject_df[
            subject_column_name] else 0
        percentages.append(data_type_similarity)

        # Calculate a similarity percentage with set operations (intersection vs union) for (possible) categorical data
        if not pandas.api.types.is_numeric_dtype(reference_column) and not pandas.api.types.is_numeric_dtype(
                subject_column):
            categorical_similarity = len(numpy.intersect1d(reference_column, subject_column)) / len(
                numpy.union1d(reference_column, subject_column)) * 100
            percentages.append(categorical_similarity)

        # Calculate the Pearson correlation coefficient between the 2 columns if they are both numerical
        if pandas.api.types.is_numeric_dtype(reference_column) and pandas.api.types.is_numeric_dtype(subject_column):
            pearson_correlation_coefficient = abs(reference_column.corr(subject_column)) * 100
            percentages.append(pearson_correlation_coefficient)

        # Produce a similarity percentage as an average of all checks
        return round(sum(percentages) / len(percentages), 2)

    @staticmethod
    def match_column_names():
        """
        Calculates similarities between all pairs of column names in a list
        :return:
        """

        headers = ['']
        lcs_cells = [names]
        levenshtein_cells = [names]
        wordnet_cells = [names]

        for i in range(0, len(names)):
            headers.append(names[i])
            lcs_column = []
            levenshtein_column = []
            wordnet_column = []

            top_3_lcs = [('', 0), ('', 0), ('', 0)]
            top_3_levenshtein = [('', 0), ('', 0), ('', 0)]
            top_3_wordnet = [('', 0), ('', 0), ('', 0)]

            for j in range(0, len(names)):
                first = names[i]
                second = names[j]

                lcs_ratio = SequenceMatcher(None, first, second).ratio() * 100
                levenshtein_ratio = ratio(first, second) * 100

                synset_first = wordnet.synsets(first)
                synset_second = wordnet.synsets(second)
                wordnet_ratio = 0
                if len(synset_first) > 0 and len(synset_second) > 0:
                    wordnet_ratio = max(wordnet.wup_similarity(s1, s2) for s1, s2 in product(synset_first, synset_second)) * 100

                lcs_column.append(f"{round(lcs_ratio, 2)}%")
                levenshtein_column.append(f"{round(levenshtein_ratio, 2)}%")
                wordnet_column.append(f"{round(wordnet_ratio, 2)}")

                if i != j:
                    if lcs_ratio > top_3_lcs[0][1]:
                        top_3_lcs[0] = second, round(lcs_ratio, 2)
                    elif lcs_ratio > top_3_lcs[1][1]:
                        top_3_lcs[1] = second, round(lcs_ratio, 2)
                    elif lcs_ratio > top_3_lcs[2][1]:
                        top_3_lcs[2] = second, round(lcs_ratio, 2)

                    if levenshtein_ratio > top_3_levenshtein[0][1]:
                        top_3_levenshtein[0] = second, round(levenshtein_ratio, 2)
                    elif levenshtein_ratio > top_3_levenshtein[1][1]:
                        top_3_levenshtein[1] = second, round(levenshtein_ratio, 2)
                    elif levenshtein_ratio > top_3_levenshtein[2][1]:
                        top_3_levenshtein[2] = second, round(levenshtein_ratio, 2)

                    if wordnet_ratio > top_3_wordnet[0][1]:
                        top_3_wordnet[0] = second, round(wordnet_ratio, 2)
                    elif wordnet_ratio > top_3_wordnet[1][1]:
                        top_3_wordnet[1] = second, round(wordnet_ratio, 2)
                    elif wordnet_ratio > top_3_wordnet[2][1]:
                        top_3_wordnet[2] = second, round(wordnet_ratio, 2)

            print(f"{names[i]}:")
            print(
                f"\t{top_3_lcs[0][0]} {top_3_lcs[0][1]}%, {top_3_lcs[1][0]} {top_3_lcs[1][1]}%, {top_3_lcs[2][0]} {top_3_lcs[2][1]}% [lcs]")
            print(
                f"\t{top_3_levenshtein[0][0]} {top_3_levenshtein[0][1]}%, {top_3_levenshtein[1][0]} {top_3_levenshtein[1][1]}%, {top_3_levenshtein[2][0]} {top_3_levenshtein[2][1]}% [levenshtein]")
            print(
                f"\t{top_3_wordnet[0][0]} {top_3_wordnet[0][1]}%, {top_3_wordnet[1][0]} {top_3_wordnet[1][1]}%, {top_3_wordnet[2][0]} {top_3_wordnet[2][1]}% [wordnet]")
            print()

            lcs_cells.append(lcs_column)
            levenshtein_cells.append(levenshtein_column)
            wordnet_cells.append(wordnet_column)

        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            specs=[[{"type": "table"}],
                   [{"type": "table"}],
                   [{"type": "table"}]]
        )
        fig.add_trace(go.Table(header=dict(values=headers),
                               cells=dict(values=lcs_cells)), row=1, col=1)
        fig.add_trace(go.Table(header=dict(values=headers),
                               cells=dict(values=levenshtein_cells)), row=2, col=1)
        fig.add_trace(go.Table(header=dict(values=headers),
                               cells=dict(values=wordnet_cells)), row=3, col=1)
        # fig.show()
